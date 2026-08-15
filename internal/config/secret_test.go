package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// 비밀 값이 어떤 출력 경로로도 새지 않아야 한다.
// event_salt 유출은 샤드 배정을 예측 가능하게 만들어 방어의 1차 전제를 무너뜨린다(§3.1).
func TestSecretNeverLeaks(t *testing.T) {
	const plaintext = "supersecretsalt"
	s := NewSecret([]byte(plaintext))

	tests := []struct {
		name   string
		render func() string
	}{
		{"String", func() string { return s.String() }},
		{"fmt %v", func() string { return fmt.Sprintf("%v", s) }},
		{"fmt %s", func() string { return fmt.Sprintf("%s", s) }},
		{"fmt %q", func() string { return fmt.Sprintf("%q", s) }},
		{"fmt %x", func() string { return fmt.Sprintf("%x", s) }},
		{"fmt %#v", func() string { return fmt.Sprintf("%#v", s) }},
		{"struct field %v", func() string {
			return fmt.Sprintf("%v", struct {
				Salt Secret
				N    int
			}{s, 3})
		}},
		{"json", func() string {
			b, err := json.Marshal(struct {
				Salt Secret `json:"salt"`
			}{s})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			return string(b)
		}},
		{"slog", func() string {
			var buf bytes.Buffer
			slog.New(slog.NewJSONHandler(&buf, nil)).Info("cfg", slog.Any("salt", s))
			return buf.String()
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.render()
			if strings.Contains(out, plaintext) {
				t.Fatalf("secret leaked in %s output: %s", tc.name, out)
			}
			if !strings.Contains(out, redactedMark) {
				t.Fatalf("expected %q in output, got %s", redactedMark, out)
			}
		})
	}
}

func TestSecretBytesIsCopy(t *testing.T) {
	s := NewSecret([]byte("abc"))
	b := s.Bytes()
	b[0] = 'z'
	if got := string(s.Bytes()); got != "abc" {
		t.Fatalf("caller mutated secret: %q", got)
	}
}

func TestParseSecretHex(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantLen int
		wantErr bool
	}{
		{"valid", "00ff10", 3, false},
		{"empty", "", 0, true},
		{"odd length", "abc", 0, true},
		{"non hex", "zz", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParseSecretHex(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				// 에러 메시지에도 원문이 들어가면 안 된다.
				if tc.in != "" && strings.Contains(err.Error(), tc.in) {
					t.Fatalf("error message leaked input: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if s.Len() != tc.wantLen {
				t.Fatalf("len = %d, want %d", s.Len(), tc.wantLen)
			}
		})
	}
}
