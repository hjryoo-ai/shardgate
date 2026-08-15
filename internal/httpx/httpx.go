// Package httpx 는 서비스들이 공유하는 HTTP 유틸리티다.
// JSON 응답 규약, 미들웨어, graceful shutdown 서버를 담는다.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxBodyBytes 는 요청 본문 크기 상한이다. 텔레메트리 배치가 가장 크다.
const maxBodyBytes = 1 << 20 // 1MiB

// ErrorBody 는 모든 오류 응답의 공통 형태다.
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Details any    `json:"details,omitempty"`
}

// APIError 는 HTTP 상태코드와 기계가 읽는 코드가 붙은 오류다.
type APIError struct {
	Status  int
	Code    string
	Message string
	Details any
}

func (e *APIError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// NewAPIError 는 APIError 를 만든다.
func NewAPIError(status int, code, message string) *APIError {
	return &APIError{Status: status, Code: code, Message: message}
}

// WriteJSON 은 값을 JSON 으로 직렬화해 응답한다.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// 헤더를 이미 보냈으므로 상태코드는 바꿀 수 없다. 로깅은 미들웨어가 한다.
		_ = err
	}
}

// WriteError 는 오류를 표준 형태로 응답한다.
func WriteError(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		apiErr = &APIError{Status: http.StatusInternalServerError, Code: "internal", Message: "internal error"}
	}
	WriteJSON(w, apiErr.Status, ErrorBody{Error: apiErr.Message, Code: apiErr.Code, Details: apiErr.Details})
}

// DecodeJSON 은 요청 본문을 크기 제한과 함께 디코딩한다.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &APIError{Status: http.StatusBadRequest, Code: "bad_request", Message: "malformed JSON body"}
	}
	// 본문에 두 번째 JSON 문서가 붙어 있는 경우를 막는다.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &APIError{Status: http.StatusBadRequest, Code: "bad_request", Message: "body must contain a single JSON object"}
	}
	return nil
}

// Handler 는 오류를 반환할 수 있는 핸들러다. WriteError 로 자동 변환된다.
type Handler func(http.ResponseWriter, *http.Request) error

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h(w, r); err != nil {
		WriteError(w, err)
	}
}
