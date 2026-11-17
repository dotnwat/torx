package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dotnwat/torx/examples/litestream/model"
)

const (
	walHeaderSize      = 32
	walFrameHeaderSize = 24
)

// walChecksum is SQLite's WAL checksum of b, continuing s0 and s1.
func walChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

// walCommitEnd is the offset just past the last frame of the WAL in b that
// commits a transaction: past it, nothing is committed, by the frames'
// own account.
func walCommitEnd(b []byte) (end int64, pageSize int, err error) {
	if len(b) < walHeaderSize {
		return 0, 0, errors.New("the WAL has no header")
	}
	var bo binary.ByteOrder = binary.LittleEndian
	if binary.BigEndian.Uint32(b[0:])&1 == 1 {
		bo = binary.BigEndian
	}
	pageSize = int(binary.BigEndian.Uint32(b[8:]))
	if pageSize == 1 {
		pageSize = 65536
	}
	s0, s1 := walChecksum(bo, 0, 0, b[:24])
	if s0 != binary.BigEndian.Uint32(b[24:]) || s1 != binary.BigEndian.Uint32(b[28:]) {
		return 0, 0, errors.New("the WAL header's checksum is wrong")
	}
	end = walHeaderSize
	frame := walFrameHeaderSize + pageSize
	for off := walHeaderSize; off+frame <= len(b); off += frame {
		fh := b[off : off+walFrameHeaderSize]
		if !bytes.Equal(fh[8:16], b[16:24]) {
			break // another generation's frame
		}
		s0, s1 = walChecksum(bo, s0, s1, fh[:8])
		s0, s1 = walChecksum(bo, s0, s1, b[off+walFrameHeaderSize:off+frame])
		if s0 != binary.BigEndian.Uint32(fh[16:]) || s1 != binary.BigEndian.Uint32(fh[20:]) {
			break
		}
		if binary.BigEndian.Uint32(fh[4:]) != 0 {
			end = int64(off + frame)
		}
	}
	return end, pageSize, nil
}

// tearReport is what tear prints.
type tearReport struct {
	Offset int64 `json:"offset"` // where the frames were written
	Frames int   `json:"frames"`
	N      int64 `json:"n"` // the txlog row the transaction took in the copy
}

func tear(args []string) error {
	fs := flag.NewFlagSet("tear", flag.ContinueOnError)
	path := fs.String("db", "", "database path")
	txnJSON := fs.String("txn", "", "the transaction, a model.Txn as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var txn model.Txn
	if err := json.Unmarshal([]byte(*txnJSON), &txn); err != nil {
		return fmt.Errorf("-txn: %w", err)
	}
	wal, err := os.ReadFile(*path + "-wal")
	if err != nil {
		return err
	}
	end, pageSize, err := walCommitEnd(wal)
	if err != nil {
		return err
	}
	if end == walHeaderSize {
		return errors.New("the WAL holds no committed frame to follow")
	}
	db, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(*path), ".tear-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	copyPath := filepath.Join(tmp, "copy.db")
	if err := os.WriteFile(copyPath, db, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(copyPath+"-wal", wal[:end], 0o644); err != nil {
		return err
	}

	// Commit the transaction to the copy, and take the frames it wrote
	// before the connection closes: the last connection to close
	// checkpoints the WAL and removes it.
	cdb, err := sql.Open("sqlite", "file:"+copyPath+"?_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)&_txlock=immediate")
	if err != nil {
		return err
	}
	defer func() { _ = cdb.Close() }()
	cdb.SetMaxOpenConns(1)
	a := &app{db: cdb}
	n, err := a.run(context.Background(), txn)
	if err != nil {
		return fmt.Errorf("commit to the copy: %w", err)
	}
	after, err := os.ReadFile(copyPath + "-wal")
	if err != nil {
		return err
	}
	if !bytes.Equal(after[:walHeaderSize], wal[:walHeaderSize]) {
		return errors.New("the copy restarted its WAL; tear needs a WAL that goes on")
	}
	frames := after[end:]
	if len(frames) == 0 || len(frames)%(walFrameHeaderSize+pageSize) != 0 {
		return fmt.Errorf("the copy wrote %d bytes of frames", len(frames))
	}

	f, err := os.OpenFile(*path+"-wal", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(frames, end); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(tearReport{Offset: end, Frames: len(frames) / (walFrameHeaderSize + pageSize), N: n})
}
