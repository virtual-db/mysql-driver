package probe

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

func RunAuthProxy(
	clientConn net.Conn,
	sourceAddr string,
	timeout time.Duration,
) (user, db string, capabilities uint32, err error) {
	deadline := time.Now().Add(timeout)
	if deadlineErr := clientConn.SetDeadline(deadline); deadlineErr != nil {
		return "", "", 0, fmt.Errorf("probe: set client deadline: %w", deadlineErr)
	}
	defer func() {
		if err == nil {
			clientConn.SetDeadline(time.Time{}) //nolint:errcheck
		}
	}()
	probeConn, dialErr := net.DialTimeout("tcp", sourceAddr, timeout)
	if dialErr != nil {
		return "", "", 0, fmt.Errorf("probe: dial %s: %w", sourceAddr, dialErr)
	}
	probeConn.SetDeadline(deadline) //nolint:errcheck
	defer probeConn.Close()
	greeting, err := readGreeting(probeConn)
	if err != nil {
		return "", "", 0, fmt.Errorf("probe: read greeting: %w", err)
	}
	if _, err = clientConn.Write(greeting); err != nil {
		return "", "", 0, fmt.Errorf("probe: forward greeting to client: %w", err)
	}
	resp, user, db, capabilities, err := readHandshakeResponse(clientConn)
	if err != nil {
		return "", "", 0, fmt.Errorf("probe: read handshake response: %w", err)
	}
	if _, err = probeConn.Write(resp); err != nil {
		return "", "", 0, fmt.Errorf("probe: forward handshake response to source: %w", err)
	}
	for {
		pkt, readErr := readRawPacket(probeConn)
		if readErr != nil {
			return "", "", 0, fmt.Errorf("probe: read auth result: %w", readErr)
		}
		if len(pkt) < 5 {
			return "", "", 0, fmt.Errorf("probe: auth result packet too short (%d bytes)", len(pkt))
		}
		firstByte := pkt[4]
		switch firstByte {
		case 0x00:
			if _, writeErr := clientConn.Write(pkt); writeErr != nil {
				return "", "", 0, fmt.Errorf("probe: forward OK to client: %w", writeErr)
			}
			return user, db, capabilities, nil
		case 0xff:
			clientConn.Write(pkt) //nolint:errcheck
			return "", "", 0, fmt.Errorf("probe: source MySQL rejected authentication")
		default:
			if _, writeErr := clientConn.Write(pkt); writeErr != nil {
				return "", "", 0, fmt.Errorf("probe: forward auth_more_data to client: %w", writeErr)
			}
			clientReply, replyErr := readRawPacket(clientConn)
			if replyErr != nil {
				return "", "", 0, fmt.Errorf("probe: read client auth reply: %w", replyErr)
			}
			if _, writeErr := probeConn.Write(clientReply); writeErr != nil {
				return "", "", 0, fmt.Errorf("probe: forward client auth reply to source: %w", writeErr)
			}
		}
	}
}

func readGreeting(conn net.Conn) ([]byte, error) {
	pkt, err := readRawPacket(conn)
	if err != nil { return nil, err }
	if len(pkt) < 5 { return nil, fmt.Errorf("greeting packet too short (%d bytes)", len(pkt)) }
	body := pkt[4:]
	nullPos := -1
	for i := 1; i < len(body); i++ {
		if body[i] == 0 { nullPos = i; break }
	}
	if nullPos < 0 { return nil, fmt.Errorf("greeting: server version string not null-terminated") }
	lowerCapsOff := nullPos + 1 + 4 + 8 + 1
	if lowerCapsOff+1 >= len(body) { return nil, fmt.Errorf("greeting: too short to contain capability flags") }
	const serverSSLHighNibble = byte(0x08)
	body[lowerCapsOff+1] &^= serverSSLHighNibble
	return pkt, nil
}

func readHandshakeResponse(conn net.Conn) (raw []byte, user, db string, caps uint32, err error) {
	raw, err = readRawPacket(conn)
	if err != nil { return }
	body := raw[4:]
	if len(body) < 32 { err = fmt.Errorf("handshake response body too short (%d bytes)", len(body)); return }
	caps = binary.LittleEndian.Uint32(body[0:4])
	pos := 32
	end := indexByte(body, pos, 0)
	if end < 0 { err = fmt.Errorf("handshake response: username not null-terminated"); return }
	user = string(body[pos:end])
	pos = end + 1
	const (
		capPluginAuthLenenc = uint32(1 << 21)
		capSecureConnection = uint32(1 << 15)
	)
	switch {
	case caps&capPluginAuthLenenc != 0:
		l, n := readLenEncInt(body[pos:])
		pos += n + int(l)
	case caps&capSecureConnection != 0:
		if pos >= len(body) { return }
		authLen := int(body[pos])
		pos += 1 + authLen
	default:
		end = indexByte(body, pos, 0)
		if end >= 0 { pos = end + 1 }
	}
	const capConnectWithDB = uint32(1 << 3)
	if caps&capConnectWithDB != 0 && pos < len(body) {
		end = indexByte(body, pos, 0)
		if end >= 0 { db = string(body[pos:end]) }
	}
	return
}

func readRawPacket(conn net.Conn) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, fmt.Errorf("read packet header: %w", err)
	}
	length := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	pkt := make([]byte, 4+length)
	copy(pkt[:4], hdr[:])
	if length > 0 {
		if _, err := io.ReadFull(conn, pkt[4:]); err != nil {
			return nil, fmt.Errorf("read packet body (%d bytes): %w", length, err)
		}
	}
	return pkt, nil
}

func indexByte(b []byte, from int, c byte) int {
	for i := from; i < len(b); i++ {
		if b[i] == c { return i }
	}
	return -1
}

func readLenEncInt(b []byte) (val uint64, n int) {
	if len(b) == 0 { return 0, 0 }
	switch b[0] {
	case 0xfc:
		if len(b) < 3 { return 0, 1 }
		return uint64(b[1]) | uint64(b[2])<<8, 3
	case 0xfd:
		if len(b) < 4 { return 0, 1 }
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4
	case 0xfe:
		if len(b) < 9 { return 0, 1 }
		return binary.LittleEndian.Uint64(b[1:9]), 9
	default:
		return uint64(b[0]), 1
	}
}
