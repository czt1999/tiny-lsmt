package main

import (
	"bytes"
	"fmt"
	"github.com/google/btree"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"tiny-lsmt/store"
)

func TestWAL(t *testing.T) {
	path := "/var/tmp/tiny_lsmt/wal"
	wal := store.NewWAL(path)
	wal.Clear()

	cmds := []store.Cmd{
		{store.CmdType_PUT, []byte("k1"), []byte("v1")},
		{store.CmdType_PUT, []byte("k2"), []byte("v2")},
		{store.CmdType_PUT, []byte("k3"), []byte("v3")},
	}
	for _, cmd := range cmds {
		_ = wal.AppendSave(cmd)
	}
	log.Printf("WAL keeps commands: %v", wal.Commands)

	// test restore
	wal.Close()
	wal = store.NewWAL(path)
	log.Printf("WAL keeps commands after restore: %v", wal.Commands)
}

func TestSSTable(t *testing.T) {
	newTablePath := "/var/tmp/tiny_lsmt"
	segSize := 5

	cmds := btree.New(2)
	for i := 1; i < 18; i++ {
		c := store.Cmd{
			CmdType: store.CmdType_PUT,
			Key:     []byte(fmt.Sprintf("k%.3d", i)),
			Value:   []byte(fmt.Sprintf("v%.3d", i)),
		}
		cmds.ReplaceOrInsert(store.CmdItem{Cmd: c})
	}
	for i := 10; i < 15; i++ {
		c := store.Cmd{
			CmdType: store.CmdType_REMOVE,
			Key:     []byte(fmt.Sprintf("k%.3d", i)),
		}
		cmds.ReplaceOrInsert(store.CmdItem{Cmd: c})
	}

	sst := store.NewSSTable(newTablePath, uint64(segSize), cmds)
	log.Printf("table meta %v", sst.Meta)
	for i := 1; i < 18; i++ {
		key := fmt.Sprintf("k%.3d", i)
		ci := sst.Find([]byte(key))
		log.Printf("%s : %s", key, ci.Value)
	}
	sst.Close()

	// test restore
	sst.Restore()
	log.Printf("table meta %v (restored)", sst.Meta)
	for i := 1; i < 20; i++ {
		key := fmt.Sprintf("k%.3d", i)
		ci := sst.Find([]byte(key))
		if ci != nil {
			log.Printf("%s : %s (restored)", key, ci.Value)
		} else {
			log.Printf("%s : %s (restored)", key, "")
		}
	}
	sst.Close()

	os.Remove(newTablePath)
}

func TestLSMTBase(t *testing.T) {
	lsmt := store.NewLSMTStore(store.DefaultConfig())
	_ = lsmt.Start()
	for i := 1; i < 500; i++ {
		k := fmt.Sprintf("k%3d", i)
		v := fmt.Sprintf("v%3d", i)
		MustPut(t, lsmt, k, v)
	}
	for i := 1; i < 300; i++ {
		k := fmt.Sprintf("k%3d", i)
		MustRemove(t, lsmt, k)
	}
	MustNotFound(t, lsmt, "k001")
	// should create a SSTable
	MustPut(t, lsmt, "k500", "v500")
	// restart store
	lsmt.Shutdown()
	_ = lsmt.Start()
	for i := 401; i <= 500; i++ {
		k := fmt.Sprintf("k%3d", i)
		v := fmt.Sprintf("v%3d", i)
		MustGet(t, lsmt, k, v)
	}
	lsmt.ShutdownClear()
}

func TestLSMTCompact(t *testing.T) {
	lsmt := store.NewLSMTStore(store.DefaultConfig())
	_ = lsmt.Start()
	for i := 0; i < 3; i++ {
		for j := 1; j <= 500; j++ {
			k := fmt.Sprintf("k%3d", j)
			v := fmt.Sprintf("v%d-%3d", i, j)
			MustPut(t, lsmt, k, v)
		}
	}
	lsmt.Shutdown()
	fs, _ := ioutil.ReadDir(store.DEFAULT_PATH + "/sst/")
	if len(fs) != 1 || fs[0].IsDir() {
		for _, f := range fs {
			log.Printf(f.Name())
		}
		t.Fatalf("expected 1 sst file")
	}
	_ = lsmt.Start()
	for i := 401; i <= 500; i++ {
		k := fmt.Sprintf("k%3d", i)
		v := fmt.Sprintf("v2-%3d", i)
		MustGet(t, lsmt, k, v)
	}
	lsmt.ShutdownClear()
}

func MustGet(t *testing.T, lsmt *store.LSMTStore, k string, v string) {
	if v2, err := lsmt.Get([]byte(k)); err != nil {
		panic(err)
	} else if !bytes.Equal(v2, []byte(v)) {
		t.Fatalf("get %s failed (expected: %s, got: %s)", k, v, v2)
	}
}

func MustPut(t *testing.T, lsmt *store.LSMTStore, k string, v string) {
	if err := lsmt.Put([]byte(k), []byte(v)); err != nil {
		panic(err)
	}
	if v2, err := lsmt.Get([]byte(k)); err != nil {
		panic(err)
	} else if !bytes.Equal(v2, []byte(v)) {
		t.Fatalf("put %s failed (expected: %s, got: %s)", k, v, v2)
	}
}

func MustRemove(t *testing.T, lsmt *store.LSMTStore, k string) {
	if err := lsmt.Remove([]byte(k)); err != nil {
		panic(err)
	}
	if v2, err := lsmt.Get([]byte(k)); err != nil {
		panic(err)
	} else if v2 != nil {
		t.Fatalf("remove %s failed (got: %s)", k, v2)
	}
}

func MustNotFound(t *testing.T, lsmt *store.LSMTStore, k string) {
	if v, err := lsmt.Get([]byte(k)); v != nil || err == nil {
		t.Fatalf("key %s must be not found (actual: %s)", k, v)
	} else if err != store.ErrNotFound {
		panic(err)
	}
}
