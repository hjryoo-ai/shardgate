package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
)

// readHeaderTimeout 는 slowloris 방지용 상한이다.
const readHeaderTimeout = 10 * time.Second

// Server 는 지표·헬스체크가 이미 붙은 HTTP 서버다.
type Server struct {
	mux     *http.ServeMux
	cfg     config.Service
	log     *slog.Logger
	metrics *obs.Metrics
	ready   func() error
}

// NewServer 는 공통 라우트(/healthz, /readyz, 지표)를 미리 붙인 서버를 만든다.
func NewServer(cfg config.Service, log *slog.Logger, m *obs.Metrics) *Server {
	s := &Server{
		mux:     http.NewServeMux(),
		cfg:     cfg,
		log:     log,
		metrics: m,
		ready:   func() error { return nil },
	}
	s.mux.Handle("GET "+cfg.MetricsPath, m.Handler())
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": cfg.Name})
	})
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.ready(); err != nil {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unready", "reason": err.Error()})
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s
}

// Mux 는 라우트 등록용 mux 를 반환한다. 패턴은 "METHOD /path" 형식을 쓴다.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// SetReady 는 readiness 체크 함수를 등록한다.
func (s *Server) SetReady(f func() error) { s.ready = f }

// Handle 은 오류 반환형 핸들러를 등록한다.
func (s *Server) Handle(pattern string, h Handler) { s.mux.Handle(pattern, h) }

// Run 은 SIGINT/SIGTERM 까지 서버를 돌리고 graceful shutdown 한다.
func (s *Server) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler := Chain(s.mux,
		RequestID(),
		WithLogger(s.log),
		Recover(),
		Observe(s.metrics),
	)

	srv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		// SSE 를 쓰므로 WriteTimeout 은 두지 않는다. 유휴 연결은 IdleTimeout 이 정리한다.
		IdleTimeout: 2 * time.Minute,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("listening", slog.String("addr", s.cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	}
}
