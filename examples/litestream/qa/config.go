//go:build unix

package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
)

// settings are Litestream's settings for a run: drawn from the seed --
// short intervals, small thresholds, aggressive compaction and retention,
// so a short run takes every path -- or Litestream's defaults.
type settings struct {
	MonitorInterval    time.Duration   `json:"monitor_interval,omitempty"`
	CheckpointInterval time.Duration   `json:"checkpoint_interval,omitempty"`
	BusyTimeout        time.Duration   `json:"busy_timeout,omitempty"`
	MinCheckpointPageN int             `json:"min_checkpoint_page_n,omitempty"`
	TruncatePageN      int             `json:"truncate_page_n,omitempty"`
	MaxSyncWALBytes    int64           `json:"max_sync_wal_bytes,omitempty"`
	SyncInterval       time.Duration   `json:"sync_interval,omitempty"`
	MaxSyncLTXFiles    int             `json:"max_sync_ltx_files,omitempty"`
	Levels             []time.Duration `json:"levels,omitempty"`
	SnapshotInterval   time.Duration   `json:"snapshot_interval,omitempty"`
	SnapshotRetention  time.Duration   `json:"snapshot_retention,omitempty"`
	L0Retention        time.Duration   `json:"l0_retention,omitempty"`
	L0RetentionCheck   time.Duration   `json:"l0_retention_check,omitempty"`
	ValidationInterval time.Duration   `json:"validation_interval,omitempty"`
	FollowInterval     time.Duration   `json:"follow_interval"`
}

func pick[T any](rng *rand.Rand, xs ...T) T { return xs[rng.IntN(len(xs))] }

func drawSettings(rng *rand.Rand, random bool) settings {
	if !random {
		return settings{FollowInterval: time.Second}
	}
	s := settings{
		MonitorInterval:    pick(rng, 10*time.Millisecond, 50*time.Millisecond, 200*time.Millisecond, time.Second),
		CheckpointInterval: pick(rng, time.Second, 5*time.Second, time.Minute),
		BusyTimeout:        pick(rng, 100*time.Millisecond, time.Second),
		MinCheckpointPageN: pick(rng, 50, 200, 1000),
		TruncatePageN:      pick(rng, 0, 500, 3000),
		MaxSyncWALBytes:    pick[int64](rng, 0, 64<<10, 1<<20),
		SyncInterval:       pick(rng, 10*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond, time.Second),
		MaxSyncLTXFiles:    pick(rng, 0, 1, 4),
		Levels: pick(rng,
			[]time.Duration{time.Second},
			[]time.Duration{2 * time.Second, 8 * time.Second},
			[]time.Duration{time.Second, 5 * time.Second, 15 * time.Second}),
		SnapshotInterval:   pick(rng, 5*time.Second, 15*time.Second, time.Minute),
		SnapshotRetention:  pick(rng, 20*time.Second, time.Minute, 5*time.Minute),
		L0Retention:        pick(rng, 2*time.Second, 10*time.Second, time.Minute),
		L0RetentionCheck:   pick(rng, time.Second, 5*time.Second),
		ValidationInterval: pick(rng, 0, 5*time.Second),
		FollowInterval:     pick(rng, 100*time.Millisecond, 500*time.Millisecond, time.Second),
	}
	return s
}

func drawApp(rng *rand.Rand, random bool) litestream.AppConfig {
	if !random {
		return litestream.AppConfig{PageSize: 4096, AutoVacuum: "none", CachePages: 2000, Autocheckpoint: 1000, Synchronous: "NORMAL"}
	}
	return litestream.AppConfig{
		PageSize:         pick(rng, 512, 1024, 4096, 4096, 8192, 65536),
		AutoVacuum:       pick(rng, "none", "full", "incremental"),
		CachePages:       pick(rng, 10, 100, 2000),
		Autocheckpoint:   pick(rng, 0, 100, 1000, 1000),
		Synchronous:      pick(rng, "NORMAL", "FULL"),
		JournalSizeLimit: pick[int64](rng, 0, 0, 32<<10, 1<<20),
	}
}

// replicaYAML is the replica section of a config: the S3 bucket the job
// serves, signed with access key, which names the process to the store.
func replicaYAML(endpoint, accessKey string, s settings) string {
	var b strings.Builder
	fmt.Fprintf(&b, "    replica:\n")
	fmt.Fprintf(&b, "      type: s3\n      bucket: %s\n      path: %s\n", bucket, replicaPath)
	fmt.Fprintf(&b, "      endpoint: %s\n      region: us-east-1\n      force-path-style: true\n", endpoint)
	fmt.Fprintf(&b, "      access-key-id: %s\n      secret-access-key: secret\n", accessKey)
	if s.SyncInterval > 0 {
		fmt.Fprintf(&b, "      sync-interval: %s\n", s.SyncInterval)
	}
	if s.MaxSyncLTXFiles > 0 {
		fmt.Fprintf(&b, "      max-sync-ltx-files: %d\n", s.MaxSyncLTXFiles)
	}
	if s.ValidationInterval > 0 {
		fmt.Fprintf(&b, "      validation-interval: %s\n", s.ValidationInterval)
	}
	return b.String()
}

// replicateYAML is the config of `litestream replicate` for the database
// at db, with its control socket at socket.
func replicateYAML(db, socket, endpoint, accessKey string, s settings) string {
	var b strings.Builder
	fmt.Fprintf(&b, "logging:\n  level: debug\n  stderr: true\n")
	fmt.Fprintf(&b, "socket:\n  enabled: true\n  path: %s\n", socket)
	if len(s.Levels) > 0 {
		fmt.Fprintf(&b, "levels:\n")
		for _, l := range s.Levels {
			fmt.Fprintf(&b, "  - interval: %s\n", l)
		}
	}
	if s.SnapshotInterval > 0 {
		fmt.Fprintf(&b, "snapshot:\n  interval: %s\n  retention: %s\n", s.SnapshotInterval, s.SnapshotRetention)
	}
	if s.L0Retention > 0 {
		fmt.Fprintf(&b, "l0-retention: %s\nl0-retention-check-interval: %s\n", s.L0Retention, s.L0RetentionCheck)
	}
	fmt.Fprintf(&b, "verify-compaction: true\n")
	fmt.Fprintf(&b, "dbs:\n  - path: %s\n", db)
	if s.MonitorInterval > 0 {
		fmt.Fprintf(&b, "    monitor-interval: %s\n", s.MonitorInterval)
	}
	if s.CheckpointInterval > 0 {
		fmt.Fprintf(&b, "    checkpoint-interval: %s\n", s.CheckpointInterval)
	}
	if s.BusyTimeout > 0 {
		fmt.Fprintf(&b, "    busy-timeout: %s\n", s.BusyTimeout)
	}
	if s.MinCheckpointPageN > 0 {
		fmt.Fprintf(&b, "    min-checkpoint-page-count: %d\n", s.MinCheckpointPageN)
	}
	if s.TruncatePageN > 0 {
		fmt.Fprintf(&b, "    truncate-page-n: %d\n", s.TruncatePageN)
	}
	if s.MaxSyncWALBytes > 0 {
		fmt.Fprintf(&b, "    max-sync-wal-bytes: %d\n", s.MaxSyncWALBytes)
	}
	b.WriteString(replicaYAML(endpoint, accessKey, s))
	return b.String()
}

// restoreYAML is the config a restore or a follower reads: the database's
// path, which names it, and its replica. level is the log level: a
// follower's log is read for its errors, and debug buries them.
func restoreYAML(db, endpoint, accessKey, level string, s settings) string {
	return "logging:\n  level: " + level + "\n  stderr: true\ndbs:\n  - path: " + db + "\n" + replicaYAML(endpoint, accessKey, s)
}
