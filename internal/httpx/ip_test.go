package httpx

import (
	"net/http/httptest"
	"net/netip"
	"os"
	"regexp"
	"testing"
)

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		want       string
	}{
		{"CDN 헤더 우선", map[string]string{"CF-Connecting-IP": "203.0.113.7"}, "10.0.0.1:1234", "203.0.113.7"},
		{"X-Real-IP", map[string]string{"X-Real-IP": "198.51.100.9"}, "10.0.0.1:1234", "198.51.100.9"},
		{"XFF 첫 항목", map[string]string{"X-Forwarded-For": "203.0.113.5, 10.0.0.2"}, "10.0.0.1:1234", "203.0.113.5"},
		{"헤더 없으면 RemoteAddr", nil, "192.0.2.44:5555", "192.0.2.44"},
		{"IPv6 RemoteAddr", nil, "[2001:db8::1]:443", "2001:db8::1"},
		{"잘못된 헤더는 무시", map[string]string{"X-Real-IP": "not-an-ip"}, "192.0.2.1:80", "192.0.2.1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.remoteAddr
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if got := ClientIP(r).String(); got != tc.want {
				t.Fatalf("= %q, want %q", got, tc.want)
			}
		})
	}
}

// 프록시가 두 헤더에 **같은 값**을 실어야 한다.
//
// ClientIP 는 X-Real-IP 를 X-Forwarded-For 보다 먼저 본다(위 테이블 참고). 그래서
// 흔한 관용구인 `X-Real-IP $remote_addr` 를 쓰면 두 헤더가 어긋나고, 조용히 틀린
// 쪽이 이긴다 — 전 사용자의 ip_prefix 가 프록시 네트워크의 /24 하나로 접힌다.
// 고약한 것은 신호가 사라지는 게 아니라 **모두에게 균일하게 만점이 얹힌다**는 점이다.
// 구분에는 전혀 기여하지 않으면서 정상 사용자의 점수만 밀어올리므로,
// 지표만 봐서는 고장인 줄 모른다. 실제로 §11 측정을 한 번 통째로 버렸다.
//
// 규칙을 주석이 아니라 테스트로 강제한다. 설정 파일을 읽는 테스트인 이유는
// 이 결함이 Go 코드가 아니라 배포 설정에 있었기 때문이다.
func TestProxyPassesOneClientIPVerdict(t *testing.T) {
	const conf = "../../deploy/nginx/sg_proxy.conf"
	b, err := os.ReadFile(conf)
	if err != nil {
		t.Fatalf("%s: %v", conf, err)
	}

	set := regexp.MustCompile(`(?m)^\s*proxy_set_header\s+(X-Real-IP|X-Forwarded-For)\s+(\S+?);`)
	got := map[string]string{}
	for _, m := range set.FindAllStringSubmatch(string(b), -1) {
		got[m[1]] = m[2]
	}

	for _, h := range []string{"X-Real-IP", "X-Forwarded-For"} {
		if got[h] == "" {
			t.Fatalf("%s 에 proxy_set_header %s 가 없다", conf, h)
		}
	}
	if got["X-Real-IP"] != got["X-Forwarded-For"] {
		t.Fatalf("X-Real-IP(%s) 와 X-Forwarded-For(%s) 가 다르다 — ClientIP 는 앞의 것을 먼저 보므로 "+
			"클라이언트 IP 판단이 조용히 뒤집힌다", got["X-Real-IP"], got["X-Forwarded-For"])
	}
	if got["X-Real-IP"] == "$remote_addr" {
		t.Fatal("$remote_addr 는 직전 피어(부하 생성기·CDN 엣지)이지 클라이언트가 아니다")
	}
}

// IP 는 프리픽스로만 다뤄야 한다 — 전체 주소는 저장하지도 전달하지도 않는다(불변식 6).
func TestIPPrefix(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{"IPv4 /24", "203.0.113.77", "203.0.113.0/24"},
		{"IPv6 /48", "2001:db8:1234:5678::1", "2001:db8:1234::/48"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := netip.MustParseAddr(tc.addr)
			if got := IPPrefix(addr, 24, 48); got != tc.want {
				t.Fatalf("= %q, want %q", got, tc.want)
			}
		})
	}

	if got := IPPrefix(netip.Addr{}, 24, 48); got != "" {
		t.Fatalf("invalid addr should yield empty prefix, got %q", got)
	}
}
