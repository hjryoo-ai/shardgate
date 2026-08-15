package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/hjr/shardgate/internal/obs"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyLogger
)

// RequestIDHeader 는 요청 추적 헤더 이름이다.
const RequestIDHeader = "X-Request-ID"

// Middleware 는 핸들러를 감싸는 함수다.
type Middleware func(http.Handler) http.Handler

// Chain 은 미들웨어를 왼쪽부터 바깥 순서로 적용한다.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// RequestID 는 요청 ID 를 생성/전파한다.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(RequestIDHeader)
			if id == "" {
				var b [8]byte
				_, _ = rand.Read(b[:])
				id = hex.EncodeToString(b[:])
			}
			w.Header().Set(RequestIDHeader, id)
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKeyRequestID, id)))
		})
	}
}

// RequestIDFrom 은 컨텍스트에서 요청 ID 를 꺼낸다.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID).(string)
	return id
}

// WithLogger 는 요청 컨텍스트에 로거를 심는다.
func WithLogger(base *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lg := base.With(slog.String("request_id", RequestIDFrom(r.Context())))
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKeyLogger, lg)))
		})
	}
}

// LoggerFrom 은 컨텍스트의 로거를 꺼낸다. 없으면 기본 로거.
func LoggerFrom(ctx context.Context) *slog.Logger {
	if lg, ok := ctx.Value(ctxKeyLogger).(*slog.Logger); ok {
		return lg
	}
	return slog.Default()
}

// Recover 는 패닉을 500 으로 변환한다. 대기열은 한 요청 때문에 죽으면 안 된다.
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					LoggerFrom(r.Context()).Error("panic recovered",
						slog.Any("panic", rec), slog.String("path", r.URL.Path))
					WriteError(w, NewAPIError(http.StatusInternalServerError, "internal", "internal error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Observe 는 접근 로그와 Prometheus 지표를 남긴다.
// route 는 http.ServeMux 패턴을 그대로 쓰면 카디널리티가 안전하다.
func Observe(m *obs.Metrics) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			m.HTTPInFlight.Inc()
			defer m.HTTPInFlight.Dec()

			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)

			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			m.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rw.status)).Inc()
			m.HTTPDuration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())

			lvl := slog.LevelInfo
			if rw.status >= http.StatusInternalServerError {
				lvl = slog.LevelError
			}
			LoggerFrom(r.Context()).Log(r.Context(), lvl, "http request",
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.Int("status", rw.status),
				slog.Duration("took", elapsed),
			)
		})
	}
}

// responseWriter 는 상태코드를 가로채면서 SSE 를 위한 Flush 도 전달한다.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *responseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *responseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 은 http.ResponseController 가 하부 writer 에 접근할 수 있게 한다.
func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
