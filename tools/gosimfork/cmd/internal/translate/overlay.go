package translate

import (
	"embed"
	"io/fs"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// stdOverlayFiles holds changed copies of standard library files, at their
// path below GOROOT/src. The translator reads them instead of the originals.
//
//go:embed all:_overlay
var stdOverlayFiles embed.FS

// stdOverlay returns stdOverlayFiles as a packages.Config.Overlay.
var stdOverlay = sync.OnceValue(func() map[string][]byte {
	out, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		log.Fatalf("running go env GOROOT: %s", err)
	}
	src := filepath.Join(strings.TrimSpace(string(out)), "src")

	overlay := make(map[string][]byte)
	err = fs.WalkDir(stdOverlayFiles, "_overlay", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := stdOverlayFiles.ReadFile(path)
		if err != nil {
			return err
		}
		overlay[filepath.Join(src, strings.TrimPrefix(path, "_overlay/"))] = b
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return overlay
})
