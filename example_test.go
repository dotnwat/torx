package torx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dotnwat/torx"
)

// A suite registers its jobs in init functions so that the driver can discover
// them and a worker can rebuild one from its id. The examples below register two:
// a plain job and one that expands into variants.
func init() {
	torx.Register("example.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("example.load", func() torx.Job { return &loadJob{} })
}

// smokeJob is the smallest possible job: it declares no services and passes.
type smokeJob struct{ torx.JobBase }

func (*smokeJob) Declare(*torx.JobContext) {}

func (*smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	jc.SetSummary("nothing to do")
	return nil
}

// loadJob runs once per point of a parameter matrix. Params are read in Run
// (or Declare) with the typed getters.
type loadJob struct{ torx.JobBase }

func (*loadJob) Matrix() []torx.Params {
	return torx.Matrix(map[string][]any{"clients": {1, 8}})
}

func (*loadJob) Declare(*torx.JobContext) {}

func (*loadJob) Run(ctx context.Context, jc *torx.JobContext) error {
	jc.SetSummary(fmt.Sprintf("%d clients", jc.Params.Int("clients", 1)))
	return nil
}

// Discovery expands every registered job into its variants and selects by id
// with regular expressions. Each request names the job and carries the
// parameters of one variant; results record both.
func ExampleDiscover() {
	reqs, err := torx.Discover(`^example\.`)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, r := range reqs {
		fmt.Println(r.ID, r.Params)
	}
	// Output:
	// example.load map[clients:1]
	// example.load map[clients:8]
	// example.smoke map[]
}

// Matrix takes dimensions in sorted name order, so the expansion is
// deterministic whatever order the map is written in.
func ExampleMatrix() {
	for _, p := range torx.Matrix(map[string][]any{
		"mode":    {"read", "write"},
		"clients": {1, 8},
	}) {
		fmt.Println(p.Int("clients", 0), p.String("mode", ""))
	}
	// Output:
	// 1 read
	// 1 write
	// 8 read
	// 8 write
}

// Parameters arrive as JSON, so a number may be a float64; the typed getters
// accept that and return the default for a key that is absent or of the wrong
// type.
func ExampleParams() {
	p := torx.Params{"clients": float64(8), "mode": "read", "clients-per-node": "many"}
	fmt.Println(p.Int("clients", 1))
	fmt.Println(p.Int("clients-per-node", 1))
	fmt.Println(p.String("mode", "write"))
	fmt.Println(p.Bool("verify", true))
	// Output:
	// 8
	// 1
	// read
	// true
}

// A service embeds *ServiceBase, passes itself as the per-node hooks, and
// declares its node demand. This one wants two nodes that carry the "nvme"
// label; the framework sizes the job from that demand before anything runs.
type storeService struct {
	*torx.ServiceBase
}

func newStoreService() *storeService {
	s := &storeService{}
	spec := torx.NodeSpec{Required: torx.Resources{Labels: torx.NewLabels("nvme")}}
	s.ServiceBase = torx.NewServiceBase("store", torx.Homogeneous(2, spec), s)
	return s
}

func (s *storeService) StartNode(ctx context.Context, n *torx.Node) error { return nil }
func (s *storeService) WaitNode(ctx context.Context, n *torx.Node) error  { return nil }
func (s *storeService) StopNode(ctx context.Context, n *torx.Node) error  { return nil }
func (s *storeService) CleanNode(ctx context.Context, n *torx.Node) error { return nil }

func ExampleNewServiceBase() {
	s := newStoreService()
	fmt.Println(s.Name(), "needs", s.Spec().Size(), "nodes")
	fmt.Println("nvme required:", s.Spec().Nodes[0].Required.Labels.Has("nvme"))
	// Output:
	// store needs 2 nodes
	// nvme required: true
}

// A launcher that wraps a suite needs the run directory before the run starts,
// to write its own record into it. MakeRunDir mints one the way the driver does
// for RunOptions.ResultsDir -- timestamped, unique, with root/latest repointed
// at it -- and the launcher then runs the suite with RunOptions.RunDir (or the
// -run-dir flag) naming it, so the tree looks the same either way.
func ExampleMakeRunDir() {
	root, err := os.MkdirTemp("", "results")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer func() { _ = os.RemoveAll(root) }()

	runDir, err := torx.MakeRunDir(root)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	// The launcher's record lands before anything runs, so even an interrupted
	// run says what produced it.
	if err := os.WriteFile(filepath.Join(runDir, "invocation.json"), []byte("{}\n"), 0o644); err != nil {
		fmt.Println("error:", err)
		return
	}
	// ... then torx.Run(ctx, pool, launcher, reqs, torx.RunOptions{RunDir: runDir}).

	latest, _ := os.Readlink(filepath.Join(root, "latest"))
	fmt.Println("latest points at the run:", latest == filepath.Base(runDir))
	// Output:
	// latest points at the run: true
}

// Readiness is a real check, never a sleep. WaitForPort returns once something
// accepts a TCP connection at the address, and ErrReadinessTimeout once the
// context is done first.
func ExampleWaitForPort() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println("listen:", err)
		return
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := torx.WaitForPort(ctx, ln.Addr().String()); err != nil {
		fmt.Println("not ready:", err)
		return
	}
	fmt.Println("ready")
	// Output:
	// ready
}

// WaitUntil polls an arbitrary predicate; the error it returns on a deadline
// wraps both ErrReadinessTimeout and the context's own error.
func ExampleWaitUntil() {
	attempts := 0
	ready := func(context.Context) (bool, error) {
		attempts++
		return attempts == 3, nil
	}
	if err := torx.WaitUntil(context.Background(), ready, time.Millisecond); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("ready after", attempts, "polls")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	never := func(context.Context) (bool, error) { return false, nil }
	err := torx.WaitUntil(ctx, never, time.Millisecond)
	fmt.Println(errors.Is(err, torx.ErrReadinessTimeout), errors.Is(err, context.DeadlineExceeded))
	// Output:
	// ready after 3 polls
	// true true
}

// Every error torx returns wraps one category sentinel, so callers classify
// with errors.Is and read the operation with errors.AsType, without depending
// on error text.
func ExampleWrap() {
	err := torx.Wrap(torx.ErrService, "service: start", io.ErrUnexpectedEOF)
	fmt.Println(err)
	fmt.Println(errors.Is(err, torx.ErrService), errors.Is(err, io.ErrUnexpectedEOF))
	if te, ok := errors.AsType[*torx.Error](err); ok {
		fmt.Println("op:", te.Op)
	}
	// Output:
	// service: start: torx: service lifecycle operation failed: unexpected EOF
	// true true
	// op: service: start
}

// MultiError collects the failures of best-effort steps, such as teardown, so
// that the first does not hide the rest; Err is nil when nothing was recorded.
func ExampleMultiError() {
	var errs torx.MultiError
	errs.Append(nil)
	fmt.Println(errs.Err())

	errs.Append(torx.Wrap(torx.ErrService, "stop", nil))
	errs.Append(torx.Wrap(torx.ErrBackend, "rm", nil))
	err := errs.Err()
	fmt.Println(errors.Is(err, torx.ErrService), errors.Is(err, torx.ErrBackend))
	fmt.Println(err)
	// Output:
	// <nil>
	// true true
	// stop: torx: service lifecycle operation failed
	// rm: torx: backend operation failed
}
