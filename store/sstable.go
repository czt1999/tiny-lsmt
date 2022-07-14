package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/btree"
	"io"
	"log"
	"os"
	"time"
	"tiny-lsmt/util"
)

// TableMeta holds the meta info for SSTable
type TableMeta struct {
	Ts          int64
	SegSize     uint64
	DataLen     uint64
	IdxLen      uint64
	StartKeyLen uint64
	EndKeyLen   uint64
}

const TableMetaLen = 48 // 6 * 8 byte

func (tm *TableMeta) Save(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return binary.Write(f, binary.BigEndian, tm)
}

func (tm *TableMeta) From(f *os.File) error {
	metaBytes := util.MustReadN(f, 0, TableMetaLen)
	return binary.Read(bytes.NewReader(metaBytes), binary.BigEndian, tm)
}

type Position struct {
	Start uint64
	Len   uint64
}

// IdxRangeItem represents an index range item to store in BTree and serves to look up SSTable's data by key
type IdxRangeItem struct {
	StartKey []byte
	EndKey   []byte
	SegPos   Position
}

// Less implements btree.Item interface
func (i IdxRangeItem) Less(than btree.Item) bool {
	i2 := than.(IdxRangeItem)
	return bytes.Compare(i.StartKey, i2.StartKey) < 0
}

func IdxRangeItemTreeToBytes(t *btree.BTree) []byte {
	items := make([]IdxRangeItem, t.Len())
	t.Ascend(func(i btree.Item) bool {
		item := i.(IdxRangeItem)
		items = append(items, item)
		return true
	})
	return util.MustMarshall(items)
}

func IdxRangeItemTreeFromBytes(idxBytes []byte) *btree.BTree {
	var items []IdxRangeItem
	util.MustUnmarshall(idxBytes, &items)
	t := btree.New(2)
	for _, i := range items {
		t.ReplaceOrInsert(i)
	}
	return t
}

// SSTable (Sorted String Table) organizes a part of data of LSM-Tree in a sorted way
// file structure : | Meta (fixed-size) | StartKey | EndKey | Data | SparseIndex |
//                                                         (1)    (2)
//                  (1) dataOffset()    = TableMetaLen + StartKeyLen + EndKeyLen
//                  (2) dataEndOffset() = (1) + DataLen
type SSTable struct {
	Meta        *TableMeta
	SparseIndex *btree.BTree
	StartKey    []byte
	EndKey      []byte
	Path        string
	f           *os.File
	fsize       int64
}

func NewSSTable(path string, segSize uint64, commands *btree.BTree) *SSTable {
	ts := time.Now().UnixNano() / 1000
	path += fmt.Sprintf("/%v.sst", ts)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	sst := &SSTable{
		Meta: &TableMeta{
			Ts:      ts,
			SegSize: segSize,
		},
		SparseIndex: btree.New(2),
		StartKey:    []byte{},
		EndKey:      []byte{},
		Path:        path,
		f:           f,
	}
	// write meta for padding
	if err = sst.Meta.Save(f); err != nil {
		panic(err)
	}
	// start/end key
	if commands.Len() > 0 {
		sst.StartKey = commands.Min().(CmdItem).Key
		sst.EndKey = commands.Max().(CmdItem).Key
		sst.Meta.StartKeyLen = uint64(len(sst.StartKey))
		sst.Meta.EndKeyLen = uint64(len(sst.EndKey))
		util.MustWriteAt(f, TableMetaLen, sst.StartKey)
		util.MustWriteAt(f, int64(TableMetaLen+sst.Meta.StartKeyLen), sst.EndKey)
	}
	// write data (commands)
	seg := &CmdList{cmds: make([]Cmd, 0, segSize)}
	commands.Ascend(func(i btree.Item) bool {
		c := i.(CmdItem).Cmd
		// append write command
		switch c.CmdType {
		case CmdType_PUT, CmdType_REMOVE:
			seg.cmds = append(seg.cmds, c)
		}
		// write segment data
		if uint64(len(seg.cmds)) >= sst.Meta.SegSize {
			sst.WriteSegData(seg)
			seg.cmds = make([]Cmd, 0, segSize)
		}
		return true
	})
	// some data remained
	if len(seg.cmds) > 0 {
		sst.WriteSegData(seg)
	}
	// write sparse index
	idxBytes := IdxRangeItemTreeToBytes(sst.SparseIndex)
	sst.Meta.IdxLen = uint64(len(idxBytes))
	util.MustWriteAt(f, sst.dataEndOffset(), idxBytes)
	// write final meta
	if err = sst.Meta.Save(f); err != nil {
		panic(err)
	}
	//log.Printf("[NewSSTable] save meta %v", sst.Meta)
	util.MustSync(f)
	fi, _ := f.Stat()
	sst.fsize = fi.Size()
	return sst
}

func (s *SSTable) Restore() {
	f, err := os.OpenFile(s.Path, os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	s.Meta = &TableMeta{}
	s.SparseIndex = btree.New(2)
	s.f = f
	// read meta
	if err = s.Meta.From(f); err != nil {
		panic(err)
	}
	log.Printf("[SSTable][Restore] %s read meta %v", s.Path, s.Meta)
	s.StartKey = util.MustReadN(f, TableMetaLen, s.Meta.StartKeyLen)
	s.EndKey = util.MustReadN(f, int64(TableMetaLen+s.Meta.StartKeyLen), s.Meta.EndKeyLen)
	// read sparse index
	idxBytes := util.MustReadN(f, s.dataEndOffset(), s.Meta.IdxLen)
	s.SparseIndex = IdxRangeItemTreeFromBytes(idxBytes)
	log.Printf("[SSTable][Restore] %s read index %v", s.Path, s.SparseIndex)
	fi, _ := f.Stat()
	s.fsize = fi.Size()
}

func (s *SSTable) Find(key []byte) *Cmd {
	if s.SparseIndex.Len() == 0 {
		return nil
	}
	if !util.KeyInRange(key, s.StartKey, s.EndKey) {
		return nil
	}
	var target IdxRangeItem
	// find the latest segment where the pivot may locate
	s.SparseIndex.Ascend(func(i btree.Item) bool {
		maybe := i.(IdxRangeItem)
		if bytes.Compare(maybe.StartKey, key) > 0 {
			// stop search
			return false
		}
		if bytes.Compare(maybe.StartKey, key) <= 0 && bytes.Compare(maybe.EndKey, key) >= 0 {
			// maybe located
			target = maybe
		}
		return true
	})
	// key is not in this table
	if target.StartKey == nil {
		return nil
	}
	log.Printf("[SSTable][Find] %s key %s locates at %v [%s, %s]", s.Path, key, target.SegPos.Start, target.StartKey, target.EndKey)
	// read segment data with certainty
	data := &CmdList{}
	data.FromBytes(util.MustReadN(s.f, int64(target.SegPos.Start), target.SegPos.Len))
	return data.Find(key)
}

func (s *SSTable) Data() *CmdList {
	data := &CmdList{}
	data.FromBytes(util.MustReadN(s.f, s.dataOffset(), s.Meta.DataLen))
	return data
}

func (s *SSTable) WriteSegData(data *CmdList) {
	if len(data.cmds) == 0 {
		return
	}
	//log.Printf("[SSTable][WriteSeg] Meta.DataLen %v data %v", s.Meta.DataLen, data)
	dataBytes := data.ToBytes()
	util.MustWriteAt(s.f, s.dataEndOffset(), dataBytes)
	// update sparse index
	s.SparseIndex.ReplaceOrInsert(IdxRangeItem{
		StartKey: data.cmds[0].Key,
		EndKey:   data.cmds[len(data.cmds)-1].Key,
		SegPos:   Position{Start: uint64(s.dataEndOffset()), Len: uint64(len(dataBytes))},
	})
	// update meta
	s.Meta.DataLen += uint64(len(dataBytes))
}

func (s *SSTable) dataOffset() int64 {
	return int64(TableMetaLen + s.Meta.StartKeyLen + s.Meta.EndKeyLen)
}

func (s *SSTable) dataEndOffset() int64 {
	return int64(TableMetaLen + s.Meta.StartKeyLen + s.Meta.EndKeyLen + s.Meta.DataLen)
}

func (s *SSTable) Close() {
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			log.Printf("[SSTable][Close] close file err %v", err)
		}
	}
}
