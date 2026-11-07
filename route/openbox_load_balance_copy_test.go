package route

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/sagernet/sing/common/logger"
)

type balanceCopyTestConn struct {
	net.Conn
	reports   atomic.Int32
	wasClosed atomic.Bool
	err       error
	download  bool
}

func (c *balanceCopyTestConn) ReportCopyResult(download bool, _ int64, err error) {
	if c.wasClosed.Load() {
		panic("reported after cleanup")
	}
	if err != c.err {
		panic("wrong error")
	}
	c.download = download
	c.reports.Add(1)
}
func (c *balanceCopyTestConn) ReaderReplaceable() bool  { return false }
func (c *balanceCopyTestConn) Read([]byte) (int, error) { return 0, c.err }
func (c *balanceCopyTestConn) Close() error             { c.wasClosed.Store(true); return c.Conn.Close() }
func TestOpenBoxLoadBalanceCopyHookBeforeCleanup(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	x, y := net.Pipe()
	defer y.Close()
	c := &balanceCopyTestConn{Conn: a, err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}}
	m := &ConnectionManager{logger: logger.NOP()}
	var done atomic.Bool
	m.connectionCopy(context.Background(), c, x, true, &done, nil)
	if c.reports.Load() != 1 || !c.download || !c.wasClosed.Load() {
		t.Fatal("copy lifecycle missed outbound error")
	}
}
func TestOpenBoxLoadBalanceCopyHookIgnoresSecondaryError(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &balanceCopyTestConn{Conn: a, err: errors.New("test")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reportLoadBalanceCopy(ctx, c, b, true, 0, c.err, false)
	reportLoadBalanceCopy(context.Background(), c, b, true, 0, c.err, true)
	if c.reports.Load() != 0 {
		t.Fatal("reported cleanup/cancellation")
	}
}
