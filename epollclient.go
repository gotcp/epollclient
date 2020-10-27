package epollclient

import (
	"net"

	"golang.org/x/sys/unix"

	"github.com/wuyongjia/pool"
)

const (
	DEFAULT_KEEPALIVE     = 1
	DEFAULT_BUFFER_LENGTH = 4096
	DEFAULT_TIMEOUT       = 6
	DEFAULT_KEEP_CNT      = 2
	DEFAULT_KEEP_INTVL    = 8
)

type Conn struct {
	Id   uint64
	Fd   int
	Data interface{} // external data
}

type Connections struct {
	host         string
	port         int
	keepAlive    int
	bufferLength int
	timeout      int
	keepCnt      int
	keepIntvl    int
	pool         *pool.Pool
}

func New(host string, port int, capacity int) *Connections {
	var connections = &Connections{
		host:         host,
		port:         port,
		keepAlive:    DEFAULT_KEEPALIVE,
		bufferLength: DEFAULT_BUFFER_LENGTH,
		timeout:      DEFAULT_TIMEOUT,
		keepCnt:      DEFAULT_KEEP_CNT,
		keepIntvl:    DEFAULT_KEEP_INTVL,
	}
	connections.pool = pool.NewWithId(capacity, allocWithId)
	return connections
}

func allocWithId(id uint64) interface{} {
	var c = &Conn{
		Fd:   -1,
		Id:   id,
		Data: nil,
	}
	return c
}

func (c *Connections) SetKeepAlive(n int) {
	c.keepAlive = n
}

func (c *Connections) SetBufferLength(n int) {
	c.bufferLength = n
}

func (c *Connections) SetTimeout(n int) {
	c.timeout = n
}

func (c *Connections) SetKeepCnt(n int) {
	c.keepCnt = n
}

func (c *Connections) SetKeepIntvl(n int) {
	c.keepIntvl = n
}

func (c *Connections) Reconnect(conn *Conn) error {
	if conn.Fd >= 0 {
		unix.Close(conn.Fd)
		conn.Fd = -1
	}
	var fd, err = c.newSocketConnection()
	if err != nil {
		return err
	}
	conn.Fd = fd
	return nil
}

func (c *Connections) Get() (*Conn, error) {
	var ptr, err = c.pool.Get()
	if err == nil {
		var conn, ok = ptr.(*Conn)
		if ok {
			if conn.Fd < 0 {
				var fd int
				fd, err = c.newSocketConnection()
				if err != nil {
					c.Put(conn)
					return nil, err
				}
				conn.Fd = fd
			}
			return conn, nil
		}
	}
	return nil, err
}

func (c *Connections) Put(conn *Conn) {
	c.pool.PutWithId(conn, conn.Id)
}

func (c *Connections) newSocketConnection() (int, error) {
	var fd, err = c.socket()
	if err != nil {
		return -1, err
	}
	err = c.connect(fd)
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func (c *Connections) socket() (int, error) {
	var fd, err = unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	return fd, nil
}

func (c *Connections) connect(fd int) error {
	var addr = unix.SockaddrInet4{Port: c.port}
	copy(addr.Addr[:], net.ParseIP(c.host).To4())

	var err = unix.Connect(fd, &addr)
	if err != nil {
		return err
	}

	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, c.keepAlive)

	if c.keepAlive == 1 {
		unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.TCP_KEEPCNT, c.keepCnt)
		unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.TCP_KEEPINTVL, c.keepIntvl)
	}

	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, c.bufferLength)
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, c.bufferLength)
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, c.timeout)
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, c.timeout)

	return nil
}

func Read(fd int, msg []byte) (int, error) {
	return unix.Read(fd, msg)
}

func Write(fd int, msg []byte) (int, error) {
	return unix.Write(fd, msg)
}

func (c *Connections) Close(conn *Conn) error {
	var err error
	if conn.Fd >= 0 {
		err = unix.Close(conn.Fd)
		conn.Fd = -1
	}
	return err
}
