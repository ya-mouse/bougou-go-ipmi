package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// retryTimeoutError is a net.Error that always reports a timeout, so the
// exchangeLAN retry loop treats every read as retryable.
type retryTimeoutError struct{}

func (retryTimeoutError) Error() string   { return "retry test timeout" }
func (retryTimeoutError) Timeout() bool   { return true }
func (retryTimeoutError) Temporary() bool { return true }

type retryTestAddr struct{}

func (retryTestAddr) Network() string { return "udp" }
func (retryTestAddr) String() string  { return "retry-test" }

// retryTimeoutConn returns a timeout on every Read and succeeds on Write.
type retryTimeoutConn struct {
	readStarted chan struct{}
	once        sync.Once
}

func (c *retryTimeoutConn) Read([]byte) (int, error) {
	c.once.Do(func() { close(c.readStarted) })
	return 0, &net.OpError{Op: "read", Net: "udp", Err: retryTimeoutError{}}
}

func (c *retryTimeoutConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *retryTimeoutConn) Close() error                     { return nil }
func (c *retryTimeoutConn) LocalAddr() net.Addr              { return retryTestAddr{} }
func (c *retryTimeoutConn) RemoteAddr() net.Addr             { return retryTestAddr{} }
func (c *retryTimeoutConn) SetDeadline(time.Time) error      { return nil }
func (c *retryTimeoutConn) SetReadDeadline(time.Time) error  { return nil }
func (c *retryTimeoutConn) SetWriteDeadline(time.Time) error { return nil }

type retryTestDialer struct {
	conn  net.Conn
	calls atomic.Int32
}

func (d *retryTestDialer) Dial(string, string) (net.Conn, error) {
	d.calls.Add(1)
	return d.conn, nil
}

func newRetryTestClient(t *testing.T, timeout time.Duration) *Client {
	t.Helper()
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.WithTimeout(timeout)
	return c
}

func TestLANRetryIntervalIsAppliedBetweenAttempts(t *testing.T) {
	conn := &retryTimeoutConn{readStarted: make(chan struct{})}
	dialer := &retryTestDialer{conn: conn}
	c := newRetryTestClient(t, 100*time.Millisecond)
	c.WithUDPProxy(dialer).WithRetries(2, 50*time.Millisecond)

	start := time.Now()
	_, err := c.RmcpPing(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("RmcpPing succeeded against a timeout conn")
	}
	// retryCount=2 means three attempts and two interval sleeps.
	if elapsed < 90*time.Millisecond {
		t.Fatalf("elapsed = %v, want >= 90ms (two 50ms waits)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("elapsed = %v, retry path is not bounded", elapsed)
	}
	if got := dialer.calls.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1 (the conn is reused)", got)
	}
}

func TestLANCloseDuringRetryIntervalPreservesNetClosed(t *testing.T) {
	conn := &retryTimeoutConn{readStarted: make(chan struct{})}
	dialer := &retryTestDialer{conn: conn}
	c := newRetryTestClient(t, 100*time.Millisecond)
	c.WithUDPProxy(dialer).WithRetries(1, 5*time.Second)

	done := make(chan error, 1)
	go func() { _, err := c.RmcpPing(context.Background()); done <- err }()

	select {
	case <-conn.readStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("exchange did not reach the retry timeout")
	}
	// Give the exchange a moment to enter waitRetryInterval; the interval is
	// 5s, so Close below lands inside the sleep.
	time.Sleep(20 * time.Millisecond)

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Close(closeCtx) // the fake conn cannot answer CloseSession; ignore it

	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("in-flight exchange error = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight exchange did not stop after Close")
	}
}
