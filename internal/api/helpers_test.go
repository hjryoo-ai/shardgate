package api

import (
	"io"
	"log/slog"
)

// discardLogger 는 테스트 출력에 로그가 섞이지 않게 한다.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
