package listener

import (
	"net"
	"sync"
	"sync/atomic"
)

// ProbeFunc runs the byte-transparent auth relay for one client connection.
type ProbeFunc func(client net.Conn) (user, db string, capabilities uint32, err error)

type acceptResult struct {
	conn net.Conn
	err  error
}

type authListener struct {
	inner     net.Listener
	ready     chan acceptResult
	done      chan struct{}
	wg        sync.WaitGroup
	nextID    atomic.Uint32
	probeFunc ProbeFunc
}

func New(inner net.Listener, probe ProbeFunc) *authListener {
	l := &authListener{
		inner:     inner,
		ready:     make(chan acceptResult, 64),
		done:      make(chan struct{}),
		probeFunc: probe,
	}
	go l.loop()
	return l
}

func (l *authListener) Accept() (net.Conn, error) {
	result, ok := <-l.ready
	if !ok {
		return nil, net.ErrClosed
	}
	return result.conn, result.err
}

func (l *authListener) Close() error {
	err := l.inner.Close()
	close(l.done)
	l.wg.Wait()
	return err
}

func (l *authListener) Addr() net.Addr {
	return l.inner.Addr()
}

func (l *authListener) loop() {
	for {
		conn, err := l.inner.Accept()
		if err != nil {
			select {
			case <-l.done:
				close(l.ready)
				return
			default:
				continue
			}
		}
		l.wg.Add(1)
		go l.handle(l.nextID.Add(1), conn)
	}
}

func (l *authListener) handle(_ uint32, client net.Conn) {
	defer l.wg.Done()
	user, db, capabilities, err := l.probeFunc(client)
	if err != nil {
		client.Close()
		return
	}
	wrapped := newHandshakeShim(client, user, db, capabilities)
	l.ready <- acceptResult{conn: wrapped}
}
