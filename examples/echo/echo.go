//go:build unix

// Demo: a self-contained vertical slice of torx.
//
// One executable plays three roles. With no special first argument it is the
// driver (via torx.Main), which discovers the echo job and runs it through a
// worker subprocess; with "worker" torx.Main becomes that worker; and with
// "echo-server" it is the tiny TCP server that EchoService launches on a node.
// The echo job starts the service, connects to it, checks that a message
// round-trips, and records the byte count -- exercising the real driver/worker
// split, inherited pipes, node allocation, and service lifecycle end to end.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/dotnwat/torx"
)

func init() {
	torx.Register("demo.echo", func() torx.Job { return &echoJob{} })
}

const echoHost = "127.0.0.1"

// echoServerMain runs a tiny TCP echo server on the given port until the process
// is killed -- which is how EchoService stops it, by closing the server's output
// stream. It is the service process the demo deploys on a node.
func echoServerMain(args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "echo-server: usage: echo-server <port>")
		return 2
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(echoHost, args[0]))
	if err != nil {
		fmt.Fprintln(os.Stderr, "echo-server:", err)
		return 1
	}
	defer ln.Close()
	// Logged to stdout, which StartCaptured collects into the results tree.
	fmt.Printf("echo server listening on %s\n", net.JoinHostPort(echoHost, args[0]))
	for {
		conn, err := ln.Accept()
		if err != nil {
			return 0
		}
		go func(c net.Conn) {
			defer c.Close()
			_, _ = io.Copy(c, c)
		}(conn)
	}
}

// EchoService runs one echo server per node. It uses the default per-node
// lifecycle: StartNode launches a server on a freshly allocated port, WaitNode
// blocks until it accepts connections, StopNode terminates it, and CleanNode has
// nothing to remove since the service keeps no on-disk state.
type EchoService struct {
	*torx.ServiceBase

	mu      sync.Mutex
	servers map[string]torx.Process // node name -> running server
	addrs   map[string]string       // node name -> host:port
	ports   map[string]int          // node name -> leased port
}

// NewEchoService builds an EchoService named name that needs one node.
func NewEchoService(name string) *EchoService {
	s := &EchoService{
		servers: map[string]torx.Process{},
		addrs:   map[string]string{},
		ports:   map[string]int{},
	}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(1, torx.NodeSpec{}), s)
	return s
}

// Addr returns the address of the service's echo server, or "" before it starts.
func (s *EchoService) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.Nodes() {
		if addr, ok := s.addrs[n.Name()]; ok {
			return addr
		}
	}
	return ""
}

// StartNode launches an echo server on n, re-executing this binary in
// echo-server mode through the node's backend.
func (s *EchoService) StartNode(ctx context.Context, n *torx.Node) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	port, err := n.AllocatePort()
	if err != nil {
		return err
	}
	// StartCaptured redirects the server's output to a node-local file and
	// registers it for collection after the job.
	server, err := s.StartCaptured(ctx, n, torx.Command(exe, "echo-server", strconv.Itoa(port)))
	if err != nil {
		n.ReleasePort(port) // the server never came up; do not leak the lease
		return err
	}
	s.mu.Lock()
	s.servers[n.Name()] = server
	s.ports[n.Name()] = port
	s.addrs[n.Name()] = net.JoinHostPort(echoHost, strconv.Itoa(port))
	s.mu.Unlock()
	return nil
}

// WaitNode blocks until the server on n accepts a connection.
func (s *EchoService) WaitNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	addr := s.addrs[n.Name()]
	s.mu.Unlock()
	return torx.WaitForPort(ctx, addr)
}

// StopNode terminates the server on n, if one is running, and reclaims its
// leased port once the process is down so repeated start/stop cycles do not
// exhaust the node's port range.
func (s *EchoService) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	server := s.servers[n.Name()]
	port, hasPort := s.ports[n.Name()]
	delete(s.servers, n.Name())
	delete(s.ports, n.Name())
	delete(s.addrs, n.Name())
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	if err := server.Close(); err != nil {
		return err
	}
	if hasPort {
		n.ReleasePort(port)
	}
	return nil
}

// CleanNode has nothing to remove: the echo server keeps no persistent state.
func (s *EchoService) CleanNode(ctx context.Context, n *torx.Node) error { return nil }

// echoJob starts an EchoService, round-trips a message through it, and records
// the number of bytes echoed.
type echoJob struct {
	torx.JobBase
	echo *EchoService
}

// Declare registers the echo service the job needs.
func (j *echoJob) Declare(jc *torx.JobContext) {
	j.echo = NewEchoService("echo")
	jc.Register(j.echo)
}

// Run connects to the started echo service, verifies a message round-trips, and
// records the result.
func (j *echoJob) Run(ctx context.Context, jc *torx.JobContext) error {
	addr := j.echo.Addr()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	msg := []byte("hello torx")
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if !bytes.Equal(got, msg) {
		return fmt.Errorf("echo mismatch: sent %q, got %q", msg, got)
	}

	jc.Log("info", fmt.Sprintf("echoed %d bytes via %s", len(got), addr))
	jc.SetSummary(fmt.Sprintf("echo round-trip ok (%d bytes)", len(got)))
	return jc.Record(map[string]any{"bytes": len(got), "addr": addr})
}
