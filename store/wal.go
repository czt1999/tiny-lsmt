package store

import (
	"github.com/google/btree"
	"log"
	"os"
	"tiny-lsmt/util"
)

// WAL represents write-ahead log in LSMTStore
type WAL struct {
	Commands CmdList
	f        *os.File
	fsize    int64
}

func NewWAL(path string) *WAL {
	w := &WAL{}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	w.f = f
	w.Commands = CmdList{}
	// check file state
	fi, _ := f.Stat()
	if fi.Size() == 0 {
		return w
	}
	w.fsize = fi.Size()
	// restore file data
	w.Commands.FromBytes(util.MustReadN(f, 0, uint64(w.fsize)))
	return w
}

func (w *WAL) Save() error {
	bts := w.Commands.ToBytes()
	// clear old data
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	util.MustWriteAt(w.f, 0, bts)
	w.fsize = int64(len(bts))
	return nil
}

func (w *WAL) AppendSave(cmd Cmd) error {
	bts := w.Commands.AppendToBytes(cmd)
	util.MustWriteAt(w.f, w.fsize, bts)
	w.fsize += int64(len(bts))
	return nil
}

func (w *WAL) ToTree(bt *btree.BTree) {
	if bt != nil {
		for i := range w.Commands.cmds {
			cmd := w.Commands.cmds[i]
			bt.ReplaceOrInsert(CmdItem{Cmd: cmd})
		}
	}
}

func (w *WAL) Clear() {
	_ = w.f.Truncate(0)
	w.fsize = 0
	w.Commands.Clear()
}

func (w *WAL) Close() {
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			log.Printf("[WAL][Close] close file err %v", err)
		}
	}
}
