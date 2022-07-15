package store

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/google/btree"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"
	"tiny-lsmt/util"
)

var ErrNotFound = errors.New("record not found")

const (
	DEFAULT_PATH              = "/var/tmp/tiny_lsmt"
	DEFAULT_KEY_SIZE_LIMIT    = 256
	DEFAULT_VALUE_SIZE_LIMIT  = 4096
	DEFAULT_SSTABLE_THRESHOLD = 500
	DEFAULT_SSTABLE_SEG_SIZE  = 20
	DEFAULT_LEVEL_NUM         = 3
	DEFAULT_LEVEL_1_THRESHOLD = 1
	DEFAULT_LEVEL_2_THRESHOLD = 10
	MB                        = 1024 * 1024
)

// LSMTConfig keeps configuration info for LSMTStore
type LSMTConfig struct {
	Path             string
	KeySizeLimit     uint64 // bytes
	ValueSizeLimit   uint64 // bytes
	SSTableThreshold uint64 // commands
	SSTableSegSize   uint64 // commands
	LevelNum         uint64
	LevelThreshold   map[int]uint64 // MB
}

func DefaultConfig() *LSMTConfig {
	cfg := &LSMTConfig{
		Path:             DEFAULT_PATH,
		KeySizeLimit:     DEFAULT_KEY_SIZE_LIMIT,
		ValueSizeLimit:   DEFAULT_VALUE_SIZE_LIMIT,
		SSTableThreshold: DEFAULT_SSTABLE_THRESHOLD,
		SSTableSegSize:   DEFAULT_SSTABLE_SEG_SIZE,
		LevelNum:         DEFAULT_LEVEL_NUM,
		LevelThreshold: map[int]uint64{
			1: DEFAULT_LEVEL_1_THRESHOLD,
			2: DEFAULT_LEVEL_2_THRESHOLD,
		},
	}
	return cfg
}

// LevelRun represents one `run` in a storage level
type LevelRun struct {
	path   string
	f      *os.File
	tables map[string]*SSTable
}

func LoadLevelRun(path string) *LevelRun {
	r := &LevelRun{
		path:   path,
		tables: make(map[string]*SSTable),
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	r.f = f
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}
	fsize := fi.Size()
	var offset int64
	var nextTablePathLen int64
	for offset < fsize {
		f.Seek(offset, io.SeekStart)
		util.MustReadBigEndian(f, &nextTablePathLen)
		offset += 8
		tpath := string(util.MustReadN(f, offset, uint64(nextTablePathLen)))
		t := &SSTable{Path: tpath}
		log.Printf("[LoadLevelRun] %v restore table %v", path, tpath)
		t.Restore()
		r.tables[t.Path] = t
		offset += nextTablePathLen
	}
	return r
}

func (r *LevelRun) Sync() {
	f, err := os.OpenFile(r.path, os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	r.f = f
	var offset int64
	_ = f.Truncate(0)
	log.Printf("[LevelRun][Sync] %v begin sync", r.path)

	for tpath, t := range r.tables {
		bts := []byte(tpath)
		tablePathLen := int64(len(bts))
		f.Seek(offset, io.SeekStart)
		util.MustWriteBigEndian(f, tablePathLen)
		offset += 8
		util.MustWriteAt(f, offset, bts)
		offset += tablePathLen
		log.Printf("[LevelRun][Sync] %v save table %v [%s, %s]", r.path, tpath, t.StartKey, t.EndKey)
	}
	_ = f.Sync()
}

// Find finds the latest Cmd in this run by the given key
func (r *LevelRun) Find(key []byte) *Cmd {
	for _, t := range r.tables {
		if c := t.Find(key); c != nil {
			return c
		}
	}
	return nil
}

// Merge merges a new SSTable into this run
func (r *LevelRun) Merge(t *SSTable, cfg *LSMTConfig) error {
	if t == nil || len(t.StartKey) == 0 || len(t.EndKey) == 0 {
		return errors.New("invalid table")
	}
	log.Printf("[LevelRun][Merge] coming table [%s, %s]", t.StartKey, t.EndKey)
	mergeWith := r.Overlap(t)
	if len(mergeWith) == 0 {
		r.tables[t.Path] = t
		r.Sync()
		return nil
	}
	sort.Sort(TableSlice(mergeWith))
	log.Printf("[Merge] mergeWith (sorted):")
	for _, m := range mergeWith {
		log.Printf("[%s, %s]", m.StartKey, m.EndKey)
	}
	// should merge
	tdata := t.Data()
	for i, m := range mergeWith {
		var bound []byte
		if i < len(mergeWith)-1 {
			bound = mergeWith[i+1].StartKey
		}
		mdata := m.Data()
		merged, remained := mergeDataInOrder(tdata, mdata, bound, cfg)
		// save new table
		if merged.Len() > 0 {
			newTable := NewSSTable(cfg.Path+"/sst", cfg.SSTableSegSize, merged)
			r.tables[newTable.Path] = newTable
			log.Printf("[LevelRun][Merge] save sst %v [%s, %s] %v", newTable.Path, newTable.StartKey, newTable.EndKey, merged.Len())
		}
		// merge remained data with next table
		tdata = remained
		delete(r.tables, m.Path)
	}
	if len(tdata.cmds) > 0 {
		newTable := NewSSTable(cfg.Path+"/sst", cfg.SSTableSegSize, tdata.ToTree())
		r.tables[newTable.Path] = newTable
	}
	// save the state and remove the old
	r.Sync()
	t.Close()
	_ = os.Remove(t.Path)
	for _, m := range mergeWith {
		m.Close()
		_ = os.Remove(m.Path)
		log.Printf("[LevelRun][Merge] remove sst %v [%s, %s]", m.Path, m.StartKey, m.EndKey)
	}
	return nil
}

func mergeDataInOrder(d1, d2 *CmdList, bound []byte, cfg *LSMTConfig) (*btree.BTree, *CmdList) {
	d3 := &CmdList{cmds: make([]Cmd, 0)}
	i := 0
	j := 0
	for i < len(d1.cmds) && j < len(d2.cmds) {
		if bytes.Equal(d1.cmds[i].Key, d2.cmds[j].Key) {
			// d1 is newer
			d3.cmds = append(d3.cmds, d1.cmds[i])
			i += 1
			j += 1
		} else if bytes.Compare(d1.cmds[i].Key, d2.cmds[j].Key) < 0 {
			d3.cmds = append(d3.cmds, d1.cmds[i])
			i += 1
		} else {
			d3.cmds = append(d3.cmds, d2.cmds[j])
			j += 1
		}
	}
	for i < len(d1.cmds) {
		d3.cmds = append(d3.cmds, d1.cmds[i])
		i += 1
	}
	for j < len(d2.cmds) {
		d3.cmds = append(d3.cmds, d2.cmds[j])
		j += 1
	}
	k := 0
	for k < len(d3.cmds) {
		if (bound != nil && bytes.Compare(d3.cmds[k].Key, bound) >= 0) ||
			uint64(k) == cfg.SSTableThreshold {
			break
		}
		k += 1
	}
	merged := &CmdList{cmds: make([]Cmd, k)}
	remained := &CmdList{cmds: make([]Cmd, len(d3.cmds)-k)}
	copy(merged.cmds, d3.cmds[:k])
	copy(remained.cmds, d3.cmds[k:])
	return merged.ToTree(), remained
}

func (r *LevelRun) Overlap(t *SSTable) []*SSTable {
	overlaps := make([]*SSTable, 0)
	for _, t2 := range r.tables {
		if bytes.Compare(t2.StartKey, t.EndKey) > 0 ||
			bytes.Compare(t2.EndKey, t.StartKey) < 0 {
			continue
		}
		overlaps = append(overlaps, t2)
	}
	return overlaps
}

func (r *LevelRun) Pick() *SSTable {
	for _, t := range r.tables {
		return t
	}
	return nil
}

func (r *LevelRun) RemoveTable(tpath string) {
	if t := r.tables[tpath]; t != nil {
		t.Close()
		delete(r.tables, tpath)
		r.Sync()
		_ = os.Remove(t.Path)
	}
}

func (r *LevelRun) Size() int64 {
	var size int64
	for _, t := range r.tables {
		size += t.fsize
	}
	return size
}

// LSMTStore stores command log using Log-Struct-Merged Tree
type LSMTStore struct {
	cfg  *LSMTConfig
	wal  *WAL
	mut  *sync.RWMutex
	cmds *btree.BTree
	runs map[int]*LevelRun
	stop bool
}

func checkConfig(cfg *LSMTConfig) error {
	if cfg == nil {
		return errors.New("config must not be nil")
	}
	if cfg.LevelNum != uint64(len(cfg.LevelThreshold)+1) {
		return errors.New("LevelNum must equal to len(LevelThreshold)+1")
	}
	if _, ok := cfg.LevelThreshold[int(cfg.LevelNum)]; ok {
		return errors.New("max level must have no threshold")
	}
	return nil
}

func NewLSMTStore(cfg *LSMTConfig) *LSMTStore {
	if err := checkConfig(cfg); err != nil {
		panic(err)
	}
	l := &LSMTStore{
		cfg:  cfg,
		mut:  &sync.RWMutex{},
		cmds: btree.New(2),
		runs: make(map[int]*LevelRun, cfg.LevelNum),
	}
	return l
}

func (l *LSMTStore) Start() error {
	// check path
	util.MustMkDirIfNotExist(l.cfg.Path)
	// restore WAL
	l.wal = NewWAL(l.cfg.Path + "/wal")
	// restore commands
	l.wal.ToTree(l.cmds)
	// restore runs
	util.MustMkDirIfNotExist(l.cfg.Path + "/level_runs")
	util.MustMkDirIfNotExist(l.cfg.Path + "/sst")
	for i := 1; uint64(i) <= l.cfg.LevelNum; i++ {
		l.runs[i] = LoadLevelRun(l.cfg.Path + fmt.Sprintf("/level_runs/run_%d", i))
	}
	// start a background routine to compact runs
	go func() {
		for !l.stop {
			time.Sleep(5 * time.Second)
			l.maybeCompact()
		}
	}()
	return nil
}

func (l *LSMTStore) Get(key []byte) ([]byte, error) {
	// try memory records at first
	byKey := CmdItem{}
	byKey.Key = key
	l.mut.RLock()
	defer l.mut.RUnlock()
	if item := l.cmds.Get(byKey); item != nil {
		return item.(CmdItem).Value, nil
	}
	// search up-to-down in runs
	for i := 1; uint64(i) <= l.cfg.LevelNum; i++ {
		if cmd := l.runs[i].Find(key); cmd != nil {
			if cmd.CmdType == CmdType_REMOVE {
				return nil, nil
			} else {
				return cmd.Value, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (l *LSMTStore) Put(key []byte, val []byte) error {
	if uint64(len(key)) > l.cfg.KeySizeLimit || uint64(len(val)) > l.cfg.ValueSizeLimit {
		return errors.New("key/val over size")
	}
	return l.exec(Cmd{CmdType: CmdType_PUT, Key: key, Value: val})
}

func (l *LSMTStore) Remove(key []byte) error {
	return l.exec(Cmd{CmdType: CmdType_REMOVE, Key: key})
}

func (l *LSMTStore) exec(c Cmd) error {
	l.mut.Lock()
	defer l.mut.Unlock()
	// append to WAL
	if err := l.wal.AppendSave(c); err != nil {
		return err
	}
	// update memory records
	l.cmds.ReplaceOrInsert(CmdItem{Cmd: c})
	// check threshold
	if uint64(l.cmds.Len()) >= l.cfg.SSTableThreshold {
		newTablePath := l.cfg.Path + "/sst"
		newTable := NewSSTable(newTablePath, l.cfg.SSTableSegSize, l.cmds)
		if err := l.runs[1].Merge(newTable, l.cfg); err != nil {
			log.Printf("[LSMTree][Set] merge table failed")
			return err
		}
		l.cmds = btree.New(2)
		l.wal.Clear()
	}
	return nil
}

func (l *LSMTStore) maybeCompact() {
	for i := 1; uint64(i) < l.cfg.LevelNum; i++ {
		l.mut.Lock()
		if uint64(l.runs[i].Size()) >= l.cfg.LevelThreshold[i]*MB {
			log.Printf("[LSMTStore] compact begin [%v]", i)
			toNextLevel := l.runs[i].Pick()
			log.Printf("[LSMTStore] compact pick off %v [%v]", toNextLevel.Path, i)
			if err := l.runs[i+1].Merge(toNextLevel, l.cfg); err != nil {
				panic(err)
			}
			delete(l.runs[i].tables, toNextLevel.Path)
		}
		l.mut.Unlock()
	}
}

func (l *LSMTStore) Shutdown() {
	l.mut.Lock()
	l.stop = true
	l.cmds.Clear(false)
	l.wal.Close()
	for _, r := range l.runs {
		_ = r.f.Close()
		for _, t := range r.tables {
			_ = t.f.Close()
		}
	}
	l.mut.Unlock()
}

func (l *LSMTStore) ShutdownClear() {
	l.mut.Lock()
	l.stop = true
	l.cmds.Clear(false)
	l.wal.Close()
	l.wal.Clear()
	_ = os.Remove(l.cfg.Path + "/wal")
	for _, r := range l.runs {
		_ = r.f.Close()
		_ = os.Remove(r.path)
		for _, t := range r.tables {
			_ = t.f.Close()
			_ = os.Remove(t.Path)
		}
	}
	l.mut.Unlock()
}
