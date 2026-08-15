package httpx

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// ClientIP 는 CDN/프록시 뒤에서 실제 클라이언트 IP 를 추정한다.
// 신뢰 경계 밖의 헤더이므로 봇 판정의 단독 근거로 쓰지 않는다(§4-L4 원칙과 동일).
func ClientIP(r *http.Request) netip.Addr {
	for _, h := range []string{"CF-Connecting-IP", "True-Client-IP", "X-Real-IP"} {
		if v := strings.TrimSpace(r.Header.Get(h)); v != "" {
			if a, err := netip.ParseAddr(v); err == nil {
				return a.Unmap()
			}
		}
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		// 가장 왼쪽이 원 클라이언트. 프록시 체인은 신뢰 경계에서 정리한다고 가정.
		first, _, _ := strings.Cut(v, ",")
		if a, err := netip.ParseAddr(strings.TrimSpace(first)); err == nil {
			return a.Unmap()
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if a, err := netip.ParseAddr(host); err == nil {
		return a.Unmap()
	}
	return netip.Addr{}
}

// IPPrefix 는 IP 를 지정한 프리픽스로 잘라 문자열로 반환한다.
// 개인정보 최소화(§12-2)와 §4-L5 "IP prefix 집중" 신호 양쪽에 쓰인다.
// 전체 IP 는 어디에도 저장하지 않는다(불변식 6).
func IPPrefix(addr netip.Addr, v4bits, v6bits int) string {
	if !addr.IsValid() {
		return ""
	}
	bits := v4bits
	if addr.Is6() {
		bits = v6bits
	}
	p, err := addr.Prefix(bits)
	if err != nil {
		return ""
	}
	return p.String()
}
