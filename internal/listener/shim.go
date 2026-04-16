package listener

import (
	"encoding/binary"
	"net"
	"sync"
)

type handshakeShim struct {
	net.Conn
	mu         sync.Mutex
	writeCount int
	synth      []byte
	synthOff   int
}

const authPhaseWriteCount = 4

func newHandshakeShim(conn net.Conn, user, db string, capabilities uint32) *handshakeShim {
	return &handshakeShim{
		Conn:  conn,
		synth: buildSyntheticHandshakeResponse(user, db, capabilities),
	}
}

func (s *handshakeShim) Write(b []byte) (int, error) {
	s.mu.Lock()
	discard := s.writeCount < authPhaseWriteCount
	if discard {
		s.writeCount++
	}
	s.mu.Unlock()
	if discard {
		return len(b), nil
	}
	return s.Conn.Write(b)
}

func (s *handshakeShim) Read(b []byte) (int, error) {
	s.mu.Lock()
	remaining := len(s.synth) - s.synthOff
	if remaining > 0 {
		n := copy(b, s.synth[s.synthOff:])
		s.synthOff += n
		s.mu.Unlock()
		return n, nil
	}
	s.mu.Unlock()
	return s.Conn.Read(b)
}

func buildSyntheticHandshakeResponse(user, db string, clientCaps uint32) []byte {
	const (
		capProtocol41    = uint32(1 << 9)
		capConnectWithDB = uint32(1 << 3)
		capSecureConn    = uint32(1 << 15)
		capPluginAuth    = uint32(1 << 19)
		capSSL           = uint32(1 << 11)
		maxPacketSize    = uint32(16777216)
		charSetUTF8      = byte(33)
		authRespLen      = byte(20)
	)
	caps := clientCaps
	caps &^= capSSL
	caps |= capProtocol41
	caps |= capSecureConn
	caps |= capPluginAuth
	if db != "" {
		caps |= capConnectWithDB
	} else {
		caps &^= capConnectWithDB
	}
	var body []byte
	var capBuf [4]byte
	binary.LittleEndian.PutUint32(capBuf[:], caps)
	body = append(body, capBuf[:]...)
	var mps [4]byte
	binary.LittleEndian.PutUint32(mps[:], maxPacketSize)
	body = append(body, mps[:]...)
	body = append(body, charSetUTF8)
	body = append(body, make([]byte, 23)...)
	body = append(body, []byte(user)...)
	body = append(body, 0)
	body = append(body, authRespLen)
	body = append(body, make([]byte, int(authRespLen))...)
	if db != "" {
		body = append(body, []byte(db)...)
		body = append(body, 0)
	}
	body = append(body, []byte("mysql_native_password")...)
	body = append(body, 0)
	l := len(body)
	pkt := make([]byte, 4+l)
	pkt[0] = byte(l)
	pkt[1] = byte(l >> 8)
	pkt[2] = byte(l >> 16)
	pkt[3] = 1
	copy(pkt[4:], body)
	return pkt
}
