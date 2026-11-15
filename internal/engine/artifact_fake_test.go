package engine_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type fakeArtifactService struct {
	mu        sync.Mutex
	artifacts map[string]string
	versions  map[string]fakeArtifactVersion
	payloads  map[string][]byte
	next      int
}

type fakeArtifactVersion struct {
	id string
}

func newFakeArtifactServer(t *testing.T) *httptest.Server {
	t.Helper()
	fake := &fakeArtifactService{
		artifacts: map[string]string{},
		versions:  map[string]fakeArtifactVersion{},
		payloads:  map[string][]byte{},
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	return server
}

func (f *fakeArtifactService) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/artifacts":
		var body struct {
			RequestKey string `json:"requestKey"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RequestKey == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if id, ok := f.artifacts[body.RequestKey]; ok {
			writeFakeArtifactJSON(w, http.StatusOK, map[string]any{"id": id})
			return
		}
		f.next++
		id := fmt.Sprintf("artifact-%06d", f.next)
		f.artifacts[body.RequestKey] = id
		writeFakeArtifactJSON(w, http.StatusCreated, map[string]any{"id": id})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/artifacts/") &&
		strings.HasSuffix(r.URL.Path, "/versions"):
		artifactID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/artifacts/"), "/versions")
		var body struct {
			RequestKey string          `json:"requestKey"`
			Payload    json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RequestKey == "" ||
			len(body.Payload) == 0 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		key := artifactID + "/" + body.RequestKey
		if version, ok := f.versions[key]; ok {
			writeFakeArtifactJSON(w, http.StatusOK, map[string]any{
				"id":         version.id,
				"artifactID": artifactID,
				"versionUri": "artifact://fake/" + version.id,
			})
			return
		}
		f.next++
		id := fmt.Sprintf("version-%06d", f.next)
		f.versions[key] = fakeArtifactVersion{id: id}
		f.payloads[id] = append([]byte(nil), body.Payload...)
		writeFakeArtifactJSON(w, http.StatusCreated, map[string]any{
			"id": id, "artifactID": artifactID, "versionUri": "artifact://fake/" + id,
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/artifacts/"):
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/artifacts/"), "/")
		if len(parts) != 3 || parts[1] != "versions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		payload, ok := f.payloads[parts[2]]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeFakeArtifactJSON(w, http.StatusOK, map[string]any{
			"id": parts[2], "payload": json.RawMessage(payload),
		})
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func writeFakeArtifactJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
