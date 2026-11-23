package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"

	openai "github.com/meguminnnnnnnnn/go-openai"
)

func TestIsContextOverflowError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "openai code", err: &openai.APIError{HTTPStatusCode: 400, Code: "context_length_exceeded"}, want: true},
		{name: "explicit code without status", err: &openai.APIError{Code: "string_above_max_length"}, want: true},
		{name: "openai message", err: &openai.APIError{HTTPStatusCode: 400, Message: "Maximum context length exceeded"}, want: true},
		{name: "request body", err: &openai.RequestError{HTTPStatusCode: 400, Body: []byte("too many tokens")}, want: true},
		{name: "plain text", err: errors.New("HTTP/1.1 400 Bad request: input is too long"), want: true},
		{name: "explicit context code in wrapper", err: errors.New("provider returned context_length_exceeded"), want: true},
		{name: "requires status", err: errors.New("input is too long"), want: false},
		{name: "generic schema error", err: errors.New("400 bad request: invalid JSON parameters"), want: false},
		{name: "generic token parameter", err: errors.New("400 bad request: invalid parameter token limit"), want: false},
		{name: "rate limited", err: &openai.APIError{HTTPStatusCode: 429, Code: "context_length_exceeded"}, want: false},
		{name: "typed gateway failure", err: &openai.RequestError{HTTPStatusCode: 502, Body: []byte("context_length_exceeded")}, want: false},
		{name: "wrapped gateway failure", err: errors.New("502 Bad Gateway: context_length_exceeded"), want: false},
		{name: "wrapped unavailable", err: errors.New("status code: 503: context_length_exceeded"), want: false},
		{name: "deadline", err: errors.Join(context.DeadlineExceeded, errors.New("context_length_exceeded")), want: false},
		{name: "reset", err: syscall.ECONNRESET, want: false},
		{name: "eof", err: io.ErrUnexpectedEOF, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.err != nil {
				test.err = fmt.Errorf("provider wrapper: %w", test.err)
			}
			if got := isContextOverflowError(test.err); got != test.want {
				t.Errorf("isContextOverflowError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}
