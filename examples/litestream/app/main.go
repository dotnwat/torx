// Command lsapp is the application the Litestream suite replicates: a
// program that keeps a SQLite database in WAL mode, as Litestream requires,
// and commits the transactions the suite sends it, one HTTP request each.
//
//	lsapp serve -db PATH -listen ADDR [flags]
//	lsapp verify -db PATH [-readonly]
//
// serve opens the database, creating it with the page size and auto-vacuum
// mode given if it does not exist, and serves:
//
//	POST /txn         a model.Txn; answers {"n": N}, its row in the txlog, or
//	                  {"rolledback": true}; on an error, whether the
//	                  transaction committed after all, from the txlog
//	POST /checkpoint  {"mode": "PASSIVE"|"FULL"|"RESTART"|"TRUNCATE"}
//	POST /vacuum      {"incremental": N} or {"full": true}
//	POST /reopen      close every connection and open the database again
//	GET  /lookup?uid= the txlog row of a transaction, 0 if none
//	GET  /status      the txlog's last row
//	GET  /health
//
// verify reads a copy of the database -- a restore, a follower's -- in one
// read transaction and prints, as JSON, its integrity_check, its txlog,
// every value that is not the one its header names, and the Digest of its
// rows, for the suite to hold against its model.
//
//	lsapp tear -db PATH -txn JSON
//
// tear leaves the database as a writer killed while committing txn leaves
// it: the transaction's frames in the WAL, its commit frame among them,
// but the WAL index -- which says how much of the WAL is committed, and is
// updated after the frames are written -- not updated. It runs the
// transaction with SQLite on a copy of the database and of the WAL up to
// its last commit, and writes the frames SQLite wrote there onto the WAL
// right after its last commit, where the writer would have; the -shm file
// is not touched. Nothing may write the database meanwhile.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx/examples/litestream/model"
	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lsapp serve|verify [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	case "tear":
		err = tear(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "lsapp:", err)
		os.Exit(1)
	}
}

type config struct {
	path           string
	pageSize       int
	autoVacuum     string
	cachePages     int
	autocheckpoint int
	synchronous    string
	busyTimeout    time.Duration
	journalLimit   int64
	conns          int
}

func (c config) dsn() string {
	p := []string{
		fmt.Sprintf("_pragma=busy_timeout(%d)", c.busyTimeout.Milliseconds()),
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(" + c.synchronous + ")",
		fmt.Sprintf("_pragma=cache_size(%d)", c.cachePages),
		fmt.Sprintf("_pragma=wal_autocheckpoint(%d)", c.autocheckpoint),
		"_txlock=immediate",
	}
	if c.journalLimit != 0 {
		p = append(p, fmt.Sprintf("_pragma=journal_size_limit(%d)", c.journalLimit))
	}
	return "file:" + c.path + "?" + strings.Join(p, "&")
}

type app struct {
	cfg config
	mu  sync.RWMutex // held for reading by requests, for writing by a reopen
	db  *sql.DB
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var c config
	fs.StringVar(&c.path, "db", "", "database path")
	listen := fs.String("listen", "127.0.0.1:0", "address to serve on")
	fs.IntVar(&c.pageSize, "page-size", 4096, "page size of a new database")
	fs.StringVar(&c.autoVacuum, "auto-vacuum", "none", "auto_vacuum of a new database: none, full, or incremental")
	fs.IntVar(&c.cachePages, "cache-pages", 2000, "page cache size, in pages: a small one spills transactions to the WAL")
	fs.IntVar(&c.autocheckpoint, "autocheckpoint", 1000, "wal_autocheckpoint, in pages; 0 disables")
	fs.StringVar(&c.synchronous, "synchronous", "NORMAL", "synchronous")
	fs.DurationVar(&c.busyTimeout, "busy-timeout", 10*time.Second, "busy_timeout")
	fs.Int64Var(&c.journalLimit, "journal-size-limit", 0, "journal_size_limit in bytes; 0 leaves SQLite's default")
	fs.IntVar(&c.conns, "conns", 4, "connections")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.path == "" {
		return errors.New("-db is required")
	}
	if err := create(c); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	a := &app{cfg: c}
	if err := a.open(); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /txn", a.handleTxn)
	mux.HandleFunc("POST /checkpoint", a.handleCheckpoint)
	mux.HandleFunc("POST /vacuum", a.handleVacuum)
	mux.HandleFunc("POST /reopen", a.handleReopen)
	mux.HandleFunc("GET /lookup", a.handleLookup)
	mux.HandleFunc("GET /status", a.handleStatus)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { reply(w, map[string]bool{"ok": true}) })
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	log.Printf("lsapp: serving %s on %s", c.path, ln.Addr())
	srv := &http.Server{Handler: mux}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigs
		log.Printf("lsapp: closing")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.db.Close()
}

// create makes the database if it does not exist: its page size and
// auto-vacuum mode can be set only before its first table.
func create(c config) error {
	if _, err := os.Stat(c.path); err == nil {
		return nil
	}
	db, err := sql.Open("sqlite", "file:"+c.path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	stmts := []string{
		fmt.Sprintf("PRAGMA page_size = %d", c.pageSize),
		"PRAGMA auto_vacuum = " + c.autoVacuum,
		"PRAGMA journal_mode = WAL",
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, s := range model.Schema {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return tx.Commit()
}

func (a *app) open() error {
	db, err := sql.Open("sqlite", a.cfg.dsn())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(a.cfg.conns)
	db.SetMaxIdleConns(a.cfg.conns)
	db.SetConnMaxIdleTime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return err
	}
	// Litestream requires WAL mode; a restored database may come back in
	// another, and the DSN's pragma sets it on every connection anyway.
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		_ = db.Close()
		return err
	} else if mode != "wal" {
		_ = db.Close()
		return fmt.Errorf("journal mode is %q", mode)
	}
	a.db = db
	return nil
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// txnResult is what POST /txn answers.
type txnResult struct {
	N          int64  `json:"n,omitempty"`
	RolledBack bool   `json:"rolledback,omitempty"`
	Error      string `json:"error,omitempty"`
	// Outcome, on an error, is "committed" if the txlog holds the
	// transaction after all, "aborted" if it does not, and "unknown" if
	// the txlog could not be read.
	Outcome string `json:"outcome,omitempty"`
}

func (a *app) handleTxn(w http.ResponseWriter, r *http.Request) {
	var txn model.Txn
	if err := json.NewDecoder(r.Body).Decode(&txn); err != nil {
		fail(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	// The transaction runs to its end whether or not the client waits for
	// it: one cut short by a client that gave up would be in doubt to no
	// purpose.
	ctx := context.WithoutCancel(r.Context())
	n, err := a.run(ctx, txn)
	switch {
	case err == nil && txn.Rollback:
		reply(w, txnResult{RolledBack: true})
	case err == nil:
		reply(w, txnResult{N: n})
	default:
		res := txnResult{Error: err.Error(), Outcome: "unknown"}
		if n, lerr := a.lookup(ctx, txn.UID); lerr == nil {
			res.Outcome = "aborted"
			if n > 0 {
				res.Outcome, res.N = "committed", n
			}
		}
		fail(w, http.StatusInternalServerError, res)
	}
}

func (a *app) run(ctx context.Context, txn model.Txn) (n int64, err error) {
	tx, err := a.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE, as the DSN says
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil || txn.Rollback {
			if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) && err == nil {
				err = fmt.Errorf("rollback: %w", rerr)
			}
		}
	}()
	depth := 0
	for i, op := range txn.Ops {
		if err := apply(ctx, tx, txn.UID, op, &depth); err != nil {
			return 0, fmt.Errorf("op %d (%s %s): %w", i, op.Kind, model.TableName(op.Table), err)
		}
	}
	if err := tx.QueryRowContext(ctx, `INSERT INTO txlog (uid) VALUES (?) RETURNING n`, int64(txn.UID)).Scan(&n); err != nil {
		return 0, fmt.Errorf("txlog: %w", err)
	}
	if txn.Rollback {
		return 0, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}

func exists(ctx context.Context, tx *sql.Tx, t int) (bool, error) {
	if t < model.Permanent {
		return true, nil
	}
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, model.TableName(t)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func apply(ctx context.Context, tx *sql.Tx, uid uint64, op model.Op, depth *int) error {
	if op.Table < 0 || op.Table >= model.Tables {
		return fmt.Errorf("no table %d", op.Table)
	}
	name := model.TableName(op.Table)
	switch op.Kind {
	case model.Savepoint:
		*depth++
		_, err := tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT s%d", *depth))
		return err
	case model.RollbackTo, model.Release:
		if *depth == 0 {
			return nil
		}
		sp := fmt.Sprintf("s%d", *depth)
		*depth--
		if op.Kind == model.RollbackTo {
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO "+sp); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, "RELEASE "+sp)
		return err
	case model.Create:
		if op.Table < model.Permanent {
			return nil
		}
		_, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (k INTEGER PRIMARY KEY, v BLOB NOT NULL)", name))
		return err
	case model.Drop:
		if op.Table < model.Permanent {
			return nil
		}
		_, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+name)
		return err
	}
	if ok, err := exists(ctx, tx, op.Table); err != nil || !ok {
		return err
	}
	var err error
	switch op.Kind {
	case model.Put:
		_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v", name),
			op.Key, model.Value(uid, op.Table, op.Key, op.Size))
	case model.Del:
		_, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE k = ?", name), op.Key)
	case model.DelRange:
		_, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE k >= ? AND k < ?", name), op.Key, op.Hi)
	default:
		err = fmt.Errorf("unknown op %q", op.Kind)
	}
	return err
}

func (a *app) lookup(ctx context.Context, uid uint64) (int64, error) {
	var n int64
	err := a.db.QueryRowContext(ctx, `SELECT n FROM txlog WHERE uid = ?`, int64(uid)).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

func (a *app) handleLookup(w http.ResponseWriter, r *http.Request) {
	uid, err := strconv.ParseUint(r.URL.Query().Get("uid"), 10, 64)
	if err != nil {
		fail(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	n, err := a.lookup(r.Context(), uid)
	if err != nil {
		fail(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	reply(w, map[string]int64{"n": n})
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var n, uid int64
	err := a.db.QueryRowContext(r.Context(), `SELECT n, uid FROM txlog ORDER BY n DESC LIMIT 1`).Scan(&n, &uid)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		fail(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	reply(w, map[string]int64{"n": n, "uid": uid})
}

func (a *app) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	switch req.Mode {
	case "PASSIVE", "FULL", "RESTART", "TRUNCATE":
	default:
		fail(w, http.StatusBadRequest, map[string]any{"error": "bad mode " + req.Mode})
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	var busy, logN, ckpt int
	if err := a.db.QueryRowContext(r.Context(), "PRAGMA wal_checkpoint("+req.Mode+")").Scan(&busy, &logN, &ckpt); err != nil {
		fail(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	reply(w, map[string]int{"busy": busy, "log": logN, "checkpointed": ckpt})
}

func (a *app) handleVacuum(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Incremental int  `json:"incremental"`
		Full        bool `json:"full"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	stmt := fmt.Sprintf("PRAGMA incremental_vacuum(%d)", req.Incremental)
	if req.Full {
		stmt = "VACUUM"
	}
	if _, err := a.db.ExecContext(r.Context(), stmt); err != nil {
		fail(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	reply(w, map[string]bool{"ok": true})
}

func (a *app) handleReopen(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.db.Close(); err != nil {
		log.Printf("lsapp: close: %v", err)
	}
	if err := a.open(); err != nil {
		// Nothing can be served without the database.
		log.Fatalf("lsapp: reopen: %v", err)
	}
	reply(w, map[string]bool{"ok": true})
}

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	path := fs.String("db", "", "database path")
	readonly := fs.Bool("readonly", false, "open the database read-only: a follower's, which another process writes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := os.Stat(*path); err != nil {
		return err
	}
	dsn := "file:" + *path + "?_pragma=busy_timeout(30000)"
	if *readonly {
		dsn += "&mode=ro"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	rep, err := check(context.Background(), db)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(rep)
}

const maxBad = 20

func check(ctx context.Context, db *sql.DB) (*model.Report, error) {
	// One read transaction, so a follower's next apply waits for the end.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rep := &model.Report{TableDigests: map[string]model.Digest{}}
	if err := tx.QueryRowContext(ctx, "PRAGMA page_size").Scan(&rep.PageSize); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA page_count").Scan(&rep.PageCount); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&rep.Freelist); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&rep.JournalMode); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		rep.Integrity = append(rep.Integrity, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, "SELECT n, uid FROM txlog ORDER BY n")
	if err != nil {
		return nil, err
	}
	var want int64 = 1
	for rows.Next() {
		var n, uid int64
		if err := rows.Scan(&n, &uid); err != nil {
			return nil, err
		}
		if n != want && len(rep.TxLogGaps) < maxBad {
			rep.TxLogGaps = append(rep.TxLogGaps, want)
		}
		want = n + 1
		rep.TxLog = append(rep.TxLog, uint64(uid))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for t := range model.Tables {
		name := model.TableName(t)
		if t >= model.Permanent {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&one)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			} else if err != nil {
				return nil, err
			}
			rep.Digest.AddTable(t)
		}
		var td model.Digest
		if t >= model.Permanent {
			td.AddTable(t)
		}
		rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT k, v FROM %s ORDER BY k", name))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var k int64
			var v []byte
			if err := rows.Scan(&k, &v); err != nil {
				return nil, err
			}
			uid, err := model.Parse(t, k, v)
			if err != nil {
				if len(rep.BadValues) < maxBad {
					rep.BadValues = append(rep.BadValues, fmt.Sprintf("%s[%d]: %v", name, k, err))
				}
				uid = 0 // a digest that matches nothing
			}
			rep.Digest.AddRow(t, k, uid, len(v))
			td.AddRow(t, k, uid, len(v))
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rep.TableDigests[name] = td
	}
	return rep, nil
}
