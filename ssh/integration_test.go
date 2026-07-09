package ssh

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

// TestServiceCaptureAndCollectOverSSH exercises torx's service machinery over the
// SSH backend end to end: StartCaptured redirects a process's output to a
// node-local file (over an SSH stream), and Collect retrieves it (over SFTP) --
// the capture-and-collect path a real suite depends on, proven hermetically
// against the in-process server. It keeps the SSH backend's integration coverage
// inside torx rather than relying on a downstream suite to exercise it.
func TestServiceCaptureAndCollectOverSSH(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()

	node := torx.NewNode(torx.NodeConfig{
		Name:    "n0",
		Backend: b,
		Scratch: torx.Scratch{Root: t.TempDir()},
		Ports:   torx.NewPortAllocator(""),
	})
	svc := torx.NewServiceBase("cap", torx.Homogeneous(1, torx.NodeSpec{}), nil)

	// Start a long-lived process whose combined output is captured to a
	// node-local file over an SSH stream.
	handle, err := svc.StartCaptured(ctx, node, torx.Command("sh", "-c", "echo captured-line; sleep 30"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}

	// The captured output lands on the node; read it back over SFTP.
	logPath := node.ServiceScratch("cap").Sub("stdout.log")
	waitForContent(t, node, logPath, "captured-line")

	// Stop the process (process-group teardown over SSH), then collect the
	// registered artifact into a local directory over SFTP.
	if err := handle.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	dest := t.TempDir()
	if err := torx.Collect(ctx, node, svc.Artifacts(node), true, dest); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "stdout.log"))
	if err != nil {
		t.Fatalf("collected artifact: %v", err)
	}
	if !strings.Contains(string(data), "captured-line") {
		t.Errorf("collected content = %q, want it to contain captured-line", data)
	}
}

func waitForContent(t *testing.T, node *torx.Node, path, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := node.ReadFile(context.Background(), path); err == nil && strings.Contains(string(data), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node file %s did not contain %q in time", path, want)
}
