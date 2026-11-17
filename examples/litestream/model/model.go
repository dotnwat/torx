// Package model is the database the Litestream suite's application keeps,
// and the model of it the suite checks restores against.
//
// Every transaction has a unique id and is logged, in commit order, in the
// txlog table, so any copy of the database -- the primary, a restore, a
// follower -- says which transactions it holds and in what order. Every
// value is derived from the transaction that wrote it, its table, its key,
// and its size, and begins with those, so a value read back can be checked
// byte for byte without the suite keeping it. Replaying the transactions a
// copy's txlog names on an empty State gives what the copy must hold: its
// Digest must match the copy's.
package model

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// The tables. The permanent tables exist from the start: t0 and t3 have
// indexes, so a restore whose index and table disagree fails
// integrity_check; t2 is a WITHOUT ROWID table, an index b-tree. The
// ephemeral tables are created and dropped by transactions, which rewrites
// the schema.
const (
	Permanent = 4
	Ephemeral = 4
	Tables    = Permanent + Ephemeral
)

// TableName names table t.
func TableName(t int) string {
	if t < Permanent {
		return fmt.Sprintf("t%d", t)
	}
	return fmt.Sprintf("e%d", t-Permanent)
}

// Schema creates the permanent tables and the txlog.
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS txlog (n INTEGER PRIMARY KEY, uid INTEGER NOT NULL UNIQUE)`,
	`CREATE TABLE IF NOT EXISTS t0 (k INTEGER PRIMARY KEY, v BLOB NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS t0_len ON t0 (length(v), k)`,
	`CREATE TABLE IF NOT EXISTS t1 (k INTEGER PRIMARY KEY, v BLOB NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS t2 (k INTEGER PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID`,
	`CREATE TABLE IF NOT EXISTS t3 (k INTEGER PRIMARY KEY, v BLOB NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS t3_uid ON t3 (substr(v, 1, 8))`,
}

// The kinds of Op.
const (
	Put        = "put"      // upsert Key with a value of Size bytes
	Del        = "del"      // delete Key
	DelRange   = "delrange" // delete keys in [Key, Hi)
	Create     = "create"   // create ephemeral table Table if it does not exist
	Drop       = "drop"     // drop ephemeral table Table if it exists
	Savepoint  = "sp"       // open a savepoint
	RollbackTo = "sprb"     // roll back to the open savepoint and release it
	Release    = "sprel"    // release the open savepoint, keeping its changes
)

// Op is one statement of a transaction.
type Op struct {
	Kind  string `json:"op"`
	Table int    `json:"t,omitempty"`
	Key   int64  `json:"k,omitempty"`
	Hi    int64  `json:"hi,omitempty"`
	Size  int    `json:"n,omitempty"`
}

// Txn is a transaction: its ops, in order, and then a row of the txlog --
// unless Rollback, when it rolls back instead of committing.
type Txn struct {
	UID      uint64 `json:"uid"`
	Ops      []Op   `json:"ops"`
	Rollback bool   `json:"rollback,omitempty"`
}

// MinValue is the smallest value: its header.
const MinValue = headerSize

const (
	headerSize = 24
	magic      = "LSv"
)

// Value is the value transaction uid writes to key of table t, size bytes
// long: a header naming all four, then bytes drawn from them.
func Value(uid uint64, t int, key int64, size int) []byte {
	if size < headerSize {
		size = headerSize
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[0:], uid)
	binary.BigEndian.PutUint64(b[8:], uint64(key))
	b[16] = byte(t)
	binary.BigEndian.PutUint32(b[17:], uint32(size))
	copy(b[21:], magic)
	fill(b[headerSize:], seed(uid, t, key))
	return b
}

func seed(uid uint64, t int, key int64) uint64 {
	return mix(mix(uid^0x9e3779b97f4a7c15) ^ mix(uint64(key)) ^ uint64(t)<<56)
}

func fill(b []byte, s uint64) {
	var w [8]byte
	for i := 0; i < len(b); i += 8 {
		s += 0x9e3779b97f4a7c15
		binary.LittleEndian.PutUint64(w[:], mix(s))
		copy(b[i:], w[:])
	}
}

// mix is splitmix64's finalizer.
func mix(z uint64) uint64 {
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// Parse checks that v is a value Value made for key of table t, and says
// which transaction wrote it.
func Parse(t int, key int64, v []byte) (uid uint64, err error) {
	if len(v) < headerSize {
		return 0, fmt.Errorf("value of %d bytes is shorter than a header", len(v))
	}
	if string(v[21:24]) != magic {
		return 0, fmt.Errorf("value has no magic: % x", v[:headerSize])
	}
	uid = binary.BigEndian.Uint64(v[0:])
	if k := int64(binary.BigEndian.Uint64(v[8:])); k != key {
		return uid, fmt.Errorf("value of key %d is under key %d", k, key)
	}
	if int(v[16]) != t {
		return uid, fmt.Errorf("value of table %s is in table %s", TableName(int(v[16])), TableName(t))
	}
	if n := int(binary.BigEndian.Uint32(v[17:])); n != len(v) {
		return uid, fmt.Errorf("value of %d bytes is %d bytes long", n, len(v))
	}
	want := make([]byte, len(v)-headerSize)
	fill(want, seed(uid, t, key))
	if i := firstDiff(want, v[headerSize:]); i >= 0 {
		return uid, fmt.Errorf("value written by %d differs from what it wrote at byte %d of %d", uid, headerSize+i, len(v))
	}
	return uid, nil
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// RowHash is a row's contribution to a Digest.
func RowHash(t int, key int64, uid uint64, size int) uint64 {
	return mix(mix(mix(uint64(t)+1)^uint64(key)) ^ mix(uid) ^ uint64(size)<<40)
}

// tableHash is an ephemeral table's existence's contribution to a Digest,
// so an empty table that should not exist does not go unnoticed.
func tableHash(t int) uint64 { return mix(0xe7e7 ^ uint64(t)) }

// Digest summarizes the rows of a database: their count, and the sum of
// their RowHashes and of the ephemeral tables that exist.
type Digest struct {
	Rows int    `json:"rows"`
	Sum  uint64 `json:"sum"`
}

// AddRow adds a row to d.
func (d *Digest) AddRow(t int, key int64, uid uint64, size int) {
	d.Rows++
	d.Sum += RowHash(t, key, uid, size)
}

// AddTable adds ephemeral table t's existence to d.
func (d *Digest) AddTable(t int) { d.Sum += tableHash(t) }

type row struct {
	uid  uint64
	size int
}

// State is the database's rows, as a sequence of transactions leaves them.
type State struct {
	tables [Tables]map[int64]row // nil for an ephemeral table that does not exist
	digest Digest
}

// NewState is the empty database.
func NewState() *State {
	s := &State{}
	for t := range Permanent {
		s.tables[t] = map[int64]row{}
	}
	return s
}

// Clone copies s.
func (s *State) Clone() *State {
	c := &State{digest: s.digest}
	for t, m := range s.tables {
		if m != nil {
			c.tables[t] = make(map[int64]row, len(m))
			for k, r := range m {
				c.tables[t][k] = r
			}
		}
	}
	return c
}

// Digest is the digest of s.
func (s *State) Digest() Digest { return s.digest }

// Exists reports whether table t exists.
func (s *State) Exists(t int) bool { return s.tables[t] != nil }

// Row returns key's row of table t: the transaction that wrote it and its
// size.
func (s *State) Row(t int, key int64) (uid uint64, size int, ok bool) {
	r, ok := s.tables[t][key]
	return r.uid, r.size, ok
}

// Keys returns table t's keys, sorted.
func (s *State) Keys(t int) []int64 {
	var ks []int64
	for k := range s.tables[t] {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// Apply applies txn to s, as committing it would. A rolled-back txn
// changes nothing.
func (s *State) Apply(txn Txn) {
	if txn.Rollback {
		return
	}
	for _, op := range Effective(txn.Ops) {
		s.apply(txn.UID, op)
	}
}

func (s *State) apply(uid uint64, op Op) {
	switch op.Kind {
	case Put:
		m := s.tables[op.Table]
		if m == nil {
			return // the app skips writes to a table that does not exist
		}
		size := max(op.Size, headerSize)
		if old, ok := m[op.Key]; ok {
			s.digest.Rows--
			s.digest.Sum -= RowHash(op.Table, op.Key, old.uid, old.size)
		}
		m[op.Key] = row{uid: uid, size: size}
		s.digest.AddRow(op.Table, op.Key, uid, size)
	case Del:
		s.del(op.Table, op.Key)
	case DelRange:
		for k := range s.tables[op.Table] {
			if k >= op.Key && k < op.Hi {
				s.del(op.Table, k)
			}
		}
	case Create:
		if s.tables[op.Table] == nil {
			s.tables[op.Table] = map[int64]row{}
			s.digest.AddTable(op.Table)
		}
	case Drop:
		if m := s.tables[op.Table]; m != nil {
			for k := range m {
				s.del(op.Table, k)
			}
			s.tables[op.Table] = nil
			s.digest.Sum -= tableHash(op.Table)
		}
	}
}

func (s *State) del(t int, key int64) {
	m := s.tables[t]
	if old, ok := m[key]; ok {
		delete(m, key)
		s.digest.Rows--
		s.digest.Sum -= RowHash(t, key, old.uid, old.size)
	}
}

// TableDigests are the digests of each table of s that exists, by name.
func (s *State) TableDigests() map[string]Digest {
	out := map[string]Digest{}
	for t, m := range s.tables {
		if m == nil {
			continue
		}
		var d Digest
		if t >= Permanent {
			d.AddTable(t)
		}
		for k, r := range m {
			d.AddRow(t, k, r.uid, r.size)
		}
		out[TableName(t)] = d
	}
	return out
}

// Report is what `lsapp verify` prints of a copy of the database.
type Report struct {
	Integrity []string `json:"integrity"` // ["ok"] if intact
	TxLog     []uint64 `json:"txlog"`     // uids, in order of n
	// TxLogGaps are the first missing n of each run of n the txlog skips.
	TxLogGaps []int64 `json:"txlog_gaps,omitempty"`
	Digest    Digest  `json:"digest"`
	// TableDigests are each table's own digest, by name: the permanent
	// tables and the ephemeral ones that exist.
	TableDigests map[string]Digest `json:"table_digests"`
	BadValues    []string          `json:"bad_values,omitempty"`
	PageSize     int               `json:"page_size"`
	PageCount    int               `json:"page_count"`
	Freelist     int               `json:"freelist"`
	JournalMode  string            `json:"journal_mode"`
}

// Intact reports whether r's integrity_check passed and every value and
// txlog row is whole.
func (r *Report) Intact() bool {
	return len(r.Integrity) == 1 && r.Integrity[0] == "ok" && len(r.BadValues) == 0 && len(r.TxLogGaps) == 0
}

// Effective returns the ops of a committed transaction that take effect:
// those not inside a savepoint rolled back to. Savepoints nest.
func Effective(ops []Op) []Op {
	type frame struct{ start int }
	var out []Op
	var stack []frame
	for _, op := range ops {
		switch op.Kind {
		case Savepoint:
			stack = append(stack, frame{start: len(out)})
		case RollbackTo:
			if len(stack) > 0 {
				out = out[:stack[len(stack)-1].start]
				stack = stack[:len(stack)-1]
			}
		case Release:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		default:
			out = append(out, op)
		}
	}
	return out
}
