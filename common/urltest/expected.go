package urltest

import (
	"strconv"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

// ExpectedStatus is the set of HTTP status codes a URL test accepts as a pass
// (Open-Box 1.14.1-openbox-tcp19, GitHub #395 #482).
//
// Upstream counts any response as a pass, so a node that a site answers with 403
// or 404 (an address the site blocks or flags as abusive) looks healthy, and URL
// test groups and the panel's failover never move off it. Written like mihomo's
// expected-status: empty or "*" accepts anything (upstream behaviour, the
// default), otherwise codes and ranges joined by "/" or ",": "204", "200/204",
// "200-299/302".
type ExpectedStatus struct {
	ranges [][2]int
}

func ParseExpectedStatus(text string) (ExpectedStatus, error) {
	text = strings.TrimSpace(text)
	if text == "" || text == "*" {
		return ExpectedStatus{}, nil
	}
	var ranges [][2]int
	for _, part := range strings.FieldsFunc(text, func(r rune) bool { return r == '/' || r == ',' }) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		low, high, isRange := strings.Cut(part, "-")
		from, ok := parseStatusCode(low)
		if !ok {
			return ExpectedStatus{}, E.New("invalid expected status: ", text)
		}
		to := from
		if isRange {
			to, ok = parseStatusCode(high)
			if !ok || to < from {
				return ExpectedStatus{}, E.New("invalid expected status: ", text)
			}
		}
		ranges = append(ranges, [2]int{from, to})
	}
	if len(ranges) == 0 {
		return ExpectedStatus{}, E.New("invalid expected status: ", text)
	}
	return ExpectedStatus{ranges: ranges}, nil
}

func parseStatusCode(text string) (int, bool) {
	code, err := strconv.Atoi(strings.TrimSpace(text))
	return code, err == nil && code >= 100 && code <= 599
}

// Any reports whether every status passes (nothing expected).
func (e ExpectedStatus) Any() bool {
	return len(e.ranges) == 0
}

func (e ExpectedStatus) Match(code int) bool {
	if e.Any() {
		return true
	}
	for _, r := range e.ranges {
		if code >= r[0] && code <= r[1] {
			return true
		}
	}
	return false
}

// String is the canonical form, "" when anything passes.
func (e ExpectedStatus) String() string {
	parts := make([]string, 0, len(e.ranges))
	for _, r := range e.ranges {
		if r[0] == r[1] {
			parts = append(parts, strconv.Itoa(r[0]))
		} else {
			parts = append(parts, strconv.Itoa(r[0])+"-"+strconv.Itoa(r[1]))
		}
	}
	return strings.Join(parts, "/")
}

// ProbeLink is what Probe and Failed key a test by: the same member tested against
// the same link but expecting different statuses must not share a result. Without
// an expectation it is the link itself, so keys stay what they were before tcp19.
func ProbeLink(link string, expected ExpectedStatus) string {
	if expected.Any() {
		return link
	}
	if link == "" {
		link = "http://www.gstatic.com/generate_204"
	}
	return link + " expected=" + expected.String()
}
