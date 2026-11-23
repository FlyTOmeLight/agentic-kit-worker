package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"

	openai "github.com/meguminnnnnnnnn/go-openai"
)

const maxReactiveOverflowAttempts = 5

var ErrContextCompressionExhausted = errors.New("context compression exhausted")

var (
	rejectedStatus = regexp.MustCompile(
		`\b(?:status(?: code)?[:= ]+|http[/0-9. ]+|response code[:= ]+)([45][0-9][0-9])\b`,
	)
	bareFailureStatus = regexp.MustCompile(`(?:^|:\s)(429|50[0234])\b`)
	badRequestCode    = regexp.MustCompile(`\b400\b`)
)

func isContextOverflowError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return false
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return false
	}
	var request *openai.RequestError
	if errors.As(err, &request) && request.HTTPStatusCode != 0 {
		return request.HTTPStatusCode == http.StatusBadRequest &&
			hasOverflowPhrase(string(request.Body)+" "+fmt.Sprint(request.Err))
	}
	var api *openai.APIError
	if errors.As(err, &api) {
		code := strings.ToLower(fmt.Sprint(api.Code))
		explicit := code == "context_length_exceeded" || code == "string_above_max_length"
		return (api.HTTPStatusCode == http.StatusBadRequest || (api.HTTPStatusCode == 0 && explicit)) &&
			(explicit || hasOverflowPhrase(api.Message))
	}
	detail := strings.ToLower(err.Error())
	if bareFailureStatus.MatchString(detail) || strings.Contains(detail, "connection reset") ||
		strings.Contains(detail, "timeout") || strings.Contains(detail, "unexpected eof") {
		return false
	}
	if status := rejectedStatus.FindStringSubmatch(detail); len(status) > 0 && status[1] != "400" {
		return false
	}
	if !strings.Contains(detail, "context_length_exceeded") &&
		!strings.Contains(detail, "string_above_max_length") &&
		(strings.Contains(detail, "invalid parameter") || strings.Contains(detail, "invalid json")) {
		return false
	}
	return hasOverflowPhrase(detail) && (badRequestCode.MatchString(detail) ||
		strings.Contains(detail, "bad request") || strings.Contains(detail, "context_length_exceeded"))
}

func hasOverflowPhrase(message string) bool {
	detail := strings.ToLower(message)
	for _, phrase := range []string{
		"context_length_exceeded", "string_above_max_length", "maximum context length", "context window",
		"too many tokens", "prompt is too long", "input is too long", "exceeds the context", "token limit", "reduce the length",
	} {
		if strings.Contains(detail, phrase) {
			return true
		}
	}
	return false
}
