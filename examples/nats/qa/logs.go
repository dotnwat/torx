//go:build unix

package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/dotnwat/torx"
)

// logAnomalies are what the servers' logs say went wrong inside them: a
// data race the race detector caught, in a server built with -race, and
// a panic a server recovered from or died of. Reports with the same
// stacks are one anomaly.
func (j *chaosJob) logAnomalies(ctx context.Context) []anomaly {
	var out []anomaly
	seen := map[string]bool{}
	for _, name := range j.cluster.Procs() {
		paths, err := j.cluster.LogPaths(ctx, name)
		if err != nil {
			torx.Logf(ctx, "warn", "listing %s's logs: %v", name, err)
			continue
		}
		for _, p := range paths {
			res, err := j.cluster.Node(name).Exec(ctx, torx.Command("cat", p))
			if err != nil || res.ExitCode != 0 {
				continue
			}
			for _, r := range reports(string(res.Stdout)) {
				key := r.kind + "|" + r.signature
				if seen[key] {
					continue
				}
				seen[key] = true
				out = append(out, anomaly{Kind: r.kind, Detail: fmt.Sprintf("%s (%s):\n%s", name, p, r.text)})
			}
		}
	}
	return out
}

type report struct {
	kind, signature, text string
}

// goFrame is a frame of a Go stack trace: the function's line.
var goFrame = regexp.MustCompile(`^\s*(github\.com/nats-io/\S+|main\.\S+)\(`)

// reports finds the race detector's reports and panics in a log.
func reports(log string) []report {
	var out []report
	lines := strings.Split(log, "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		var kind string
		switch {
		case strings.HasPrefix(line, "WARNING: DATA RACE"):
			kind = "data-race"
		case strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "fatal error: "):
			kind = "panic"
		default:
			continue
		}
		end := i + 1
		for end < len(lines) && end < i+120 && !strings.HasPrefix(lines[end], "==================") {
			end++
		}
		text := strings.Join(lines[i:end], "\n")
		var frames []string
		for _, l := range lines[i:end] {
			if m := goFrame.FindStringSubmatch(l); m != nil {
				frames = append(frames, m[1])
			}
			if len(frames) >= 6 {
				break
			}
		}
		out = append(out, report{kind: kind, signature: strings.Join(frames, ";"), text: text})
		i = end
	}
	return out
}
