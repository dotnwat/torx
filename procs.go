//go:build unix

package torx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ErrNotRunning is returned by the Procs methods that act on a running
// process when the named process is not running.
var ErrNotRunning = errors.New("torx: process not running")

// Procs keeps the processes a service launches by name, on the nodes it
// picks, through the life of a job: a chaos test crashes one, pauses
// another, stops a third the way an operator would, and starts each again
// later, and needs to know at the end which of them exited on their own.
//
// A name is a slot. Launch fills it with a process started on a node with a
// command; once the process is gone -- crashed, stopped, or exited on its
// own -- Restart runs the same command again on the same node, and Relaunch
// another. Each process logs to <name>.log in the service's scratch on its
// node, through StartCapturedAs, so the service's CapturePolicy says what
// becomes of a previous launch's log; CaptureRotate keeps every one.
//
// A slot holds one process at a time. A process on its way out -- one
// Terminate is still waiting on, one whose exit is still being recorded --
// is not running, but the slot is not free either: Restart and Relaunch
// wait until it is settled, so the next launch never runs beside the last,
// nor takes its log before the record of its exit has read it.
//
// A process outlives the context it was launched or restarted with: it
// runs until Procs crashes or stops it, or it exits, so a supervisor may
// restart one under a context that ends long before the job does. The
// service's StopNode -- which calls Procs.StopNode -- stops what is left at
// teardown. The context bounds only the launch itself.
//
// A process that exits without Procs having stopped it is recorded as a
// ProcExit, with the end of what it logged, and reported to OnExit: a
// server that died of a panic, an assertion, or a fatal error is a finding
// even if nothing the job checked noticed. A process whose kill could not
// be carried out -- by Crash, by Terminate, of a launch called off as its
// context ended, or after it exited or its node lost track of it -- may
// still be running, and its slot refuses a Restart.
type Procs struct {
	svc *ServiceBase

	// OnExit, if set, is called with every ProcExit as it is recorded,
	// outside any lock. Set it before the first Launch.
	OnExit func(ProcExit)
	// TailBytes bounds how much of its log a ProcExit keeps; 16 KiB if
	// zero. Only the last TailLines lines of that are kept, 20 if zero.
	TailBytes int
	TailLines int

	mu    sync.Mutex
	slots map[string]*procSlot
	exits []ProcExit
}

// ProcExit is a process that exited without Procs having stopped it.
type ProcExit struct {
	Name string    `json:"name"`
	Node string    `json:"node"`
	Code int       `json:"code"` // -1 for a process ended by a signal
	Err  string    `json:"error,omitempty"`
	Time time.Time `json:"time"`
	// Launch is which launch of the slot it was, from 1.
	Launch int `json:"launch"`
	// Tail is the end of what the process logged.
	Tail string `json:"tail,omitempty"`
}

func (e ProcExit) String() string {
	s := fmt.Sprintf("%s on %s exited with status %d", e.Name, e.Node, e.Code)
	if e.Err != "" {
		s += " (" + e.Err + ")"
	}
	return s
}

type procSlot struct {
	name     string
	node     *Node
	cmd      Cmd
	proc     Process // nil while not running
	paused   bool
	launches int
	// settling is set from when a launch is taken out of the slot to be
	// stopped, or because it exited, until that is done with: the stop
	// has returned, or what the launch left has been killed and its exit
	// recorded. It is closed then. The slot does not start again before.
	settling chan struct{}
	// closing is set once StopNode has taken the slot, to stop what it
	// holds and forget it: it does not start again.
	closing bool
	// unstopped is the error of a kill that could not be carried out: the
	// process may still run.
	unstopped error
}

// takeLocked takes the slot's process out of it, for the caller to finish
// with outside the lock, and holds the slot until settleLocked.
func (s *procSlot) takeLocked() {
	s.proc, s.paused = nil, false
	s.settling = make(chan struct{})
}

// settleLocked frees the slot takeLocked held. unstopped, if not nil, is
// the error of a kill that could not be carried out.
func (s *procSlot) settleLocked(unstopped error) {
	if unstopped != nil {
		s.unstopped = unstopped
	}
	close(s.settling)
	s.settling = nil
}

// NewProcs keeps processes for svc, whose StartCapturedAs launches them.
func NewProcs(svc *ServiceBase) *Procs {
	return &Procs{svc: svc, slots: map[string]*procSlot{}}
}

// Launch starts cmd on n as process name, a slot that must not exist yet.
// name must be a valid StartCapturedAs name.
func (p *Procs) Launch(ctx context.Context, name string, n *Node, cmd Cmd) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.slots[name]; ok {
		return fmt.Errorf("torx: process %s exists", name)
	}
	s := &procSlot{name: name, node: n, cmd: cmd}
	err := p.startLocked(ctx, s)
	// A launch that failed leaves no slot, unless it left a process it
	// could not kill: that one may still run, so the name stays taken, and
	// StopNode has its error to return.
	if err == nil || s.unstopped != nil {
		p.slots[name] = s
	}
	return err
}

// Restart starts process name again, with the command it was last
// launched with. It must not be running; one on its way out is waited
// for, as long as ctx allows.
func (p *Procs) Restart(ctx context.Context, name string) error {
	s, err := p.lockStopped(ctx, name)
	if err != nil {
		return err
	}
	defer p.mu.Unlock()
	return p.startLocked(ctx, s)
}

// Relaunch starts process name again, on the same node, with cmd. It must
// not be running; one on its way out is waited for, as long as ctx allows.
func (p *Procs) Relaunch(ctx context.Context, name string, cmd Cmd) error {
	s, err := p.lockStopped(ctx, name)
	if err != nil {
		return err
	}
	defer p.mu.Unlock()
	old := s.cmd
	s.cmd = cmd
	if err := p.startLocked(ctx, s); err != nil {
		s.cmd = old
		return err
	}
	return nil
}

// lockStopped returns process name's slot once it can start again, with
// p.mu held. It waits for a launch that is still settling: one started
// over it would run beside a process still in its grace period, or beside
// whatever a kill yet to fail has left, and would move the log aside
// before the record of the exit has read it.
func (p *Procs) lockStopped(ctx context.Context, name string) (*procSlot, error) {
	for {
		p.mu.Lock()
		s := p.slots[name]
		var err error
		switch {
		case s == nil:
			err = fmt.Errorf("torx: no process %s", name)
		case s.closing:
			err = fmt.Errorf("torx: process %s is being stopped with its node", name)
		case s.settling != nil:
			settling := s.settling
			p.mu.Unlock()
			select {
			case <-settling:
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("torx: process %s is still being stopped: %w", name, context.Cause(ctx))
			}
		case s.unstopped != nil:
			err = fmt.Errorf("torx: %s cannot start over a process that may still run: %w", name, s.unstopped)
		case s.proc != nil:
			err = fmt.Errorf("torx: process %s is running", name)
		default:
			return s, nil
		}
		p.mu.Unlock()
		return nil, err
	}
}

func (p *Procs) startLocked(ctx context.Context, s *procSlot) error {
	// The process dies with the context it was streamed under, so it gets
	// one of its own, which ctx cancels only while the launch is under way.
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	tp, err := p.svc.StartCapturedAs(life, s.node, s.name, s.cmd)
	if !stop() && err == nil {
		// ctx ended as the launch went through, so the launch is called
		// off and its process killed. One that cannot be killed may still
		// run, as after any kill that failed.
		err = context.Cause(ctx)
		if cerr := tp.Close(); cerr != nil {
			s.unstopped = fmt.Errorf("torx: calling off the launch of %s: %w", s.name, cerr)
			err = errors.Join(err, s.unstopped)
		}
	}
	if err != nil {
		cancel()
		return err
	}
	s.launches++
	s.proc, s.paused = tp, false
	go p.watch(s, tp, s.launches, cancel)
	return nil
}

// watch waits for a launch to exit, and records the exit if Procs did not
// cause it.
func (p *Procs) watch(s *procSlot, tp Process, launch int, release context.CancelFunc) {
	code, werr := tp.Wait(context.Background())
	release()
	p.mu.Lock()
	if s.proc != tp {
		p.mu.Unlock()
		return // stopped or crashed through Procs
	}
	// The slot is held until the launch is settled. The close kills what
	// the process left, or the process itself if the wait only lost track
	// of it, and a kill that fails leaves something the next launch must
	// not run beside. The log is the launch's own only until the next one
	// moves it aside or truncates it.
	s.takeLocked()
	p.mu.Unlock()
	var unstopped error
	if err := tp.Close(); err != nil {
		unstopped = fmt.Errorf("torx: cleaning up after %s: %w", s.name, err)
	}

	e := ProcExit{Name: s.name, Node: s.node.Name(), Code: code, Time: time.Now(), Launch: launch}
	if werr != nil {
		e.Err = werr.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if b, err := p.tail(ctx, s.node, s.name); err == nil {
		e.Tail = lastLines(string(b), orDefault(p.TailLines, 20))
	}
	p.mu.Lock()
	p.exits = append(p.exits, e)
	onExit := p.OnExit
	s.settleLocked(unstopped)
	p.mu.Unlock()
	if onExit != nil {
		onExit(e)
	}
}

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// running returns process name's slot and process, or ErrNotRunning.
func (p *Procs) running(name string) (*procSlot, Process, error) {
	s := p.slots[name]
	if s == nil || s.proc == nil {
		return nil, nil, fmt.Errorf("torx: %s: %w", name, ErrNotRunning)
	}
	return s, s.proc, nil
}

// Crash kills process name and its process group outright, paused or not:
// a process that dies without a chance to clean up.
func (p *Procs) Crash(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, tp, err := p.running(name)
	if err != nil {
		return err
	}
	s.proc, s.paused = nil, false
	if err := tp.Close(); err != nil {
		s.unstopped = fmt.Errorf("torx: crashing %s: %w", name, err)
		return s.unstopped
	}
	return nil
}

// Terminate stops process name as policy says (Stop): a paused process is
// resumed first, so it can act on the signal. It reports whether the
// process exited within the grace period; one that had to be killed is
// not an error. Until Terminate returns the process may still be running,
// and its slot does not start again.
//
// An error says the stop did not go as asked -- ctx ended within the grace
// period, the signal could not be sent, the node lost track of the process
// -- and the process was killed all the same, unless the kill is what
// failed: only then may it still run, and its slot refuses a Restart.
func (p *Procs) Terminate(ctx context.Context, name string, policy StopPolicy) (clean bool, err error) {
	p.mu.Lock()
	s, tp, err := p.running(name)
	if err != nil {
		p.mu.Unlock()
		return false, err
	}
	paused := s.paused
	s.takeLocked()
	p.mu.Unlock()
	if paused {
		_ = tp.Signal(ctx, syscall.SIGCONT)
	}
	// Stop ends with the kill whatever went before, and its error does not
	// say which of them failed, so the close is noted on its way through.
	stopping := &closeNoter{Process: tp}
	_, err = Stop(ctx, stopping, policy)
	timedOut := errors.Is(err, ErrShutdownTimeout)
	var unstopped error
	if stopping.err != nil {
		unstopped = fmt.Errorf("torx: stopping %s: %w", name, stopping.err)
	}
	p.mu.Lock()
	s.settleLocked(unstopped)
	p.mu.Unlock()
	if timedOut {
		return false, nil
	}
	return err == nil, err
}

// closeNoter is a Process that notes the error of its Close: whether the
// kill could be carried out.
type closeNoter struct {
	Process
	err error
}

func (c *closeNoter) Close() error {
	c.err = c.Process.Close()
	return c.err
}

// Pause stops process name with SIGSTOP: a process that goes silent
// without dying. Resume continues it. Pausing a paused process, or
// resuming one that is not, does nothing.
func (p *Procs) Pause(ctx context.Context, name string) error {
	return p.setPaused(ctx, name, true)
}

// Resume continues a paused process with SIGCONT.
func (p *Procs) Resume(ctx context.Context, name string) error {
	return p.setPaused(ctx, name, false)
}

func (p *Procs) setPaused(ctx context.Context, name string, paused bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, tp, err := p.running(name)
	if err != nil {
		return err
	}
	if s.paused == paused {
		return nil
	}
	sig := syscall.SIGCONT
	if paused {
		sig = syscall.SIGSTOP
	}
	if err := tp.Signal(ctx, sig); err != nil {
		return fmt.Errorf("torx: %v to %s: %w", sig, name, err)
	}
	s.paused = paused
	return nil
}

// Signal sends sig to process name.
func (p *Procs) Signal(ctx context.Context, name string, sig os.Signal) error {
	p.mu.Lock()
	_, tp, err := p.running(name)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return tp.Signal(ctx, sig)
}

// Running reports whether process name is running, paused or not.
func (p *Procs) Running(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[name]
	return s != nil && s.proc != nil
}

// Paused reports whether process name is paused.
func (p *Procs) Paused(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[name]
	return s != nil && s.paused
}

// Node is the node process name runs on, nil if there is no such process.
func (p *Procs) Node(name string) *Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.slots[name]; s != nil {
		return s.node
	}
	return nil
}

// Cmd is the command process name was last launched with.
func (p *Procs) Cmd(name string) Cmd {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.slots[name]; s != nil {
		return s.cmd
	}
	return Cmd{}
}

// Launches is how many times process name has been launched.
func (p *Procs) Launches(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.slots[name]; s != nil {
		return s.launches
	}
	return 0
}

// Names returns the processes' names, sorted.
func (p *Procs) Names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for name := range p.slots {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// Exits returns the processes that exited on their own, in order.
func (p *Procs) Exits() []ProcExit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.exits)
}

// Tail returns the end of what process name's last launch has logged: its
// last TailBytes bytes.
func (p *Procs) Tail(ctx context.Context, name string) ([]byte, error) {
	p.mu.Lock()
	s := p.slots[name]
	p.mu.Unlock()
	if s == nil {
		return nil, fmt.Errorf("torx: no process %s", name)
	}
	return p.tail(ctx, s.node, name)
}

func (p *Procs) tail(ctx context.Context, n *Node, name string) ([]byte, error) {
	res, err := n.Exec(ctx, Command("tail", "-c", strconv.Itoa(orDefault(p.TailBytes, 16<<10)), p.svc.CapturePath(n, name)))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("torx: reading %s's log: %s", name, strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

// StopNode stops every process on n as policy says, and forgets them: what
// a service's StopNode calls. A process already on its way out is waited
// for, and none of them starts again once StopNode is called: a Restart or
// Relaunch of one is refused, whether it was waiting for the process to be
// gone or comes from an OnExit told that it is. It returns the errors of
// the stops, and of any earlier kill that could not be carried out.
func (p *Procs) StopNode(ctx context.Context, n *Node, policy StopPolicy) error {
	p.mu.Lock()
	var mine []*procSlot
	for _, s := range p.slots {
		if s.node == n {
			// Closed to restarts before anything is stopped: one that got
			// in while a stop or an exit was waited for would run on in a
			// slot about to be forgotten.
			s.closing = true
			mine = append(mine, s)
		}
	}
	p.mu.Unlock()
	var errs []error
	for _, s := range mine {
		_, err := p.Terminate(ctx, s.name, policy)
		if errors.Is(err, ErrNotRunning) {
			err = nil
		}
		// A launch may be settling still -- in the grace period of a
		// Terminate begun elsewhere, or having exited -- and its kill may
		// yet fail: the node is not stopped, nor its errors in, before.
		p.mu.Lock()
		settling := s.settling
		p.mu.Unlock()
		if settling != nil {
			select {
			case <-settling:
			case <-ctx.Done():
				errs = append(errs, fmt.Errorf("torx: process %s is still being stopped: %w", s.name, context.Cause(ctx)))
			}
		}
		p.mu.Lock()
		unstopped := s.unstopped
		delete(p.slots, s.name)
		p.mu.Unlock()
		// A kill that failed is said once: the slot keeps it with the
		// process's name, and when it was this stop's kill, the stop's own
		// error is the same one without.
		switch {
		case unstopped != nil:
			errs = append(errs, unstopped)
		case err != nil:
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
