package route

import (
	"context"
	"net"

	"github.com/sagernet/sing/common"
)

// A lifecycle hook retains the optimized copy path. Report before local cleanup
// closes the peer and creates secondary errors in the opposite copy goroutine.
func reportLoadBalanceCopy(ctx context.Context, source, destination net.Conn, download bool, n int64, err error, closing bool) {
	if closing || err == nil && (!download || n != 0) || ctx.Err() != nil {
		return
	}
	remote := destination
	if download {
		remote = source
	}
	if reporter, ok := common.Cast[interface{ ReportCopyResult(bool, int64, error) }](remote); ok {
		reporter.ReportCopyResult(download, n, err)
	}
}

func reportLoadBalanceHandshake(ctx context.Context, outbound net.Conn, err error, closing bool) {
	if closing || err == nil || ctx.Err() != nil {
		return
	}
	if reporter, ok := common.Cast[interface{ ReportOutboundHandshakeFailure(error) }](outbound); ok {
		reporter.ReportOutboundHandshakeFailure(err)
	}
}
