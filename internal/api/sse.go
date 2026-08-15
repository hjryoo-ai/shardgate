package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// writeSSE 는 SSE 이벤트 한 건을 쓴다.
//
// JSON 을 한 줄로 직렬화해 보내는 이유: SSE 의 data 필드는 줄바꿈을 만나면 거기서
// 끊기므로, 여러 줄짜리 JSON 은 프레임을 깨뜨린다.
func writeSSE(w http.ResponseWriter, event string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("sse marshal: %w", err)
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
		return fmt.Errorf("sse write: %w", err)
	}
	return nil
}
