// Package obs 는 구조화 로깅과 Prometheus 지표를 제공한다.
package obs

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger 는 서비스 이름이 박힌 JSON slog 로거를 만든다.
// 비밀 값은 config.Secret 이 slog.LogValuer 를 구현해 자동으로 마스킹된다.
func NewLogger(level, service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: ParseLevel(level)})
	return slog.New(h).With(slog.String("service", service))
}

// ParseLevel 은 문자열 로그 레벨을 slog.Level 로 바꾼다. 알 수 없으면 info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
