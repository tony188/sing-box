package option

import "github.com/sagernet/sing/common/json/badoption"

// Local, per-connection balancing. No remote aggregation endpoint is needed.
type LoadBalanceOutboundOptions struct {
	Bandwidth      bool               `json:"bandwidth,omitempty"`
	Outbounds      []string           `json:"outbounds" reference:"outbound"`
	URL            string             `json:"url,omitempty"`
	Interval       badoption.Duration `json:"interval,omitempty"`
	ExpectedStatus string             `json:"expected_status,omitempty"`
}
