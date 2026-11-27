package engine

import (
	"fmt"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

func TestArtifactPathsAndIndex(t *testing.T) {
	fs := &durable.FilesystemBinding{Provider: "AIO", Workspace: "/workspace/project"}
	for _, tc := range []struct {
		path string
		want string
	}{
		{path: "./out/a.txt", want: "/workspace/project/out/a.txt"},
		{path: "/workspace/project/out/../a.txt", want: "/workspace/project/a.txt"},
		{path: "../other/file"},
		{path: "/workspace/project2/file"},
		{path: ""},
		{path: "out/bad\nfile"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			got := fileArtifacts(fs, "written", fmt.Sprintf(`{"file_path":%q}`, tc.path))
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("unexpected artifact: %+v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Path != tc.want || got[0].Name != "a.txt" ||
				got[0].Provider != fs.Provider {
				t.Fatalf("artifact=%+v, want path %q", got, tc.want)
			}
		})
	}

	var prior []contextbuilder.FileIndexEntry
	for i := range contextbuilder.MaxFileIndexEntries {
		prior = append(prior, contextbuilder.FileIndexEntry{URI: fmt.Sprintf("/workspace/project/file-%d", i)})
	}
	edited := durable.FileArtifact{Path: prior[1].URI, Action: "edited"}
	updated := indexArtifact(prior, edited)
	if len(updated) != len(prior) || updated[0].URI != edited.Path || updated[0].Action != "edited" ||
		updated[1] != prior[0] || updated[2] != prior[2] || prior[1].Action != "" {
		t.Fatalf(
			"editing should move and replace the existing entry without changing the prior state: %+v",
			updated,
		)
	}
	added := indexArtifact(prior, durable.FileArtifact{Path: "/workspace/project/new", Action: "written"})
	if len(added) != contextbuilder.MaxFileIndexEntries || added[len(added)-1] != prior[len(prior)-2] {
		t.Fatalf("full index should evict the oldest entry: %+v", added)
	}
}
