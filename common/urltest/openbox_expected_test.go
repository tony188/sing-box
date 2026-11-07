package urltest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Open-Box tcp19 (GitHub #395 #482):测速可以只认某些状态码。不写 / "*" 和上游一样什么应答都算通;
// 写了的话应答状态码不在里面就算不通,自动择优、故障转移据此换节点

func TestOpenBoxExpectedParse(t *testing.T) {
	for text, want := range map[string]string{
		"":                     "",
		"*":                    "",
		" * ":                  "",
		"204":                  "204",
		"200/204":              "200/204",
		"200-299/302":          "200-299/302",
		"200-399, 204":         "200-399/204",
		" 200 - 399 / 418 ":    "200-399/418",
		"100-599":              "100-599",
		"200-200":              "200",
		"200-299,,302":         "200-299/302",
		"404/200-299/301-302/": "404/200-299/301-302",
	} {
		expected, err := ParseExpectedStatus(text)
		if err != nil || expected.String() != want {
			t.Errorf("%q: got %q, %v; want %q", text, expected.String(), err, want)
		}
	}
	for _, text := range []string{"abc", "99", "600", "300-200", "/", ",", "200-", "-200", "2OO", "200-abc", "!200"} {
		if _, err := ParseExpectedStatus(text); err == nil {
			t.Errorf("%q should be rejected", text)
		}
	}
	expected, _ := ParseExpectedStatus("200-299/302")
	for code, want := range map[int]bool{200: true, 204: true, 299: true, 302: true, 301: false, 403: false, 404: false, 503: false} {
		if expected.Match(code) != want {
			t.Errorf("match %d: want %v", code, want)
		}
	}
	if any := (ExpectedStatus{}); !any.Any() || !any.Match(403) || !any.Match(503) {
		t.Error("no expectation must accept every status")
	}
	// 缓存键:不写期望时和 tcp18 一样就是地址本身;写了的话地址相同、期望不同的不能共用一次结果
	if ProbeLink("https://a.test/", ExpectedStatus{}) != "https://a.test/" || ProbeLink("", ExpectedStatus{}) != "" {
		t.Error("probe link without expectation must stay the link")
	}
	if ProbeLink("https://a.test/", expected) == ProbeLink("https://a.test/", ExpectedStatus{}) ||
		ProbeLink("", expected) != ProbeLink("http://www.gstatic.com/generate_204", expected) {
		t.Errorf("probe links: %q %q", ProbeLink("https://a.test/", expected), ProbeLink("", expected))
	}
}

func TestOpenBoxExpectedStatus(t *testing.T) {
	status := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := int(status.Load()); code >= 300 && code < 400 {
			w.Header().Set("Location", "https://must-not-follow.invalid/")
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	dialer := &openBoxHTTPDialer{address: server.Listener.Addr().String()}
	ok, _ := ParseExpectedStatus("200-399")
	only204, _ := ParseExpectedStatus("204")
	for _, c := range []struct {
		status   int
		expected ExpectedStatus
		pass     bool
	}{
		{403, ExpectedStatus{}, true}, // 和上游一样:不写期望时 403 也算通
		{404, ExpectedStatus{}, true},
		{403, ok, false},
		{404, ok, false},
		{503, ok, false},
		{200, ok, true},
		{204, ok, true},
		{302, ok, true}, // 重定向不跟,状态码本身在范围里就算通
		{200, only204, false},
		{204, only204, true},
	} {
		status.Store(int32(c.status))
		delay, err := URLTestExpected(context.Background(), server.URL+"/probe", c.expected, dialer)
		if c.pass && (err != nil || delay == 0) {
			t.Errorf("status %d expected %q: want pass, got delay=%d err=%v", c.status, c.expected, delay, err)
		}
		// 失败的原因写明是哪个状态码(面板按它提示「状态码 403」)
		if !c.pass && (err == nil || delay != 0 || !strings.Contains(err.Error(), fmt.Sprintf("unexpected status %d", c.status))) {
			t.Errorf("status %d expected %q: want failure naming the status, got delay=%d err=%v", c.status, c.expected, delay, err)
		}
	}
	// URLTest 本身不变(Clash API 不带 expected、daemon 手动测速):什么状态码都算通
	status.Store(http.StatusForbidden)
	if delay, err := URLTest(context.Background(), server.URL, dialer); err != nil || delay == 0 {
		t.Errorf("URLTest must keep accepting any status: delay=%d err=%v", delay, err)
	}
}

// 同一节点、同一地址,期望不同的两次测速各测各的,结果不串(Probe 按 ProbeLink 记)
func TestOpenBoxExpectedProbeKey(t *testing.T) {
	storage := NewHistoryStorage()
	defer storage.Close()
	expected, _ := ParseExpectedStatus("200-399")
	var runs atomic.Int32
	run := func(delay uint16) func(context.Context) (uint16, error) {
		return func(context.Context) (uint16, error) {
			runs.Add(1)
			return delay, nil
		}
	}
	link := "https://www.google.com/"
	first := storage.Probe(context.Background(), "node", ProbeLink(link, ExpectedStatus{}), time.Minute, false, run(11))
	second := storage.Probe(context.Background(), "node", ProbeLink(link, expected), time.Minute, false, run(22))
	again := storage.Probe(context.Background(), "node", ProbeLink(link, expected), time.Minute, false, run(33))
	if runs.Load() != 2 || first.Delay != 11 || second.Delay != 22 || again.Delay != 22 || !again.Reused {
		t.Fatalf("runs=%d first=%+v second=%+v again=%+v", runs.Load(), first, second, again)
	}
}
