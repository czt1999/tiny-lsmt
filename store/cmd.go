package store

import (
	"bytes"
	"fmt"
	"github.com/google/btree"
	"tiny-lsmt/util"
)

type CmdType uint64

const (
	CmdType_GET = iota
	CmdType_PUT
	CmdType_REMOVE
)

type Cmd struct {
	CmdType CmdType
	Key     []byte
	Value   []byte
}

func (c *Cmd) FromBytes(cmdBytes []byte) {
	if len(cmdBytes) <= 16 {
		err := fmt.Errorf("[Cmd][FromBytes] invalid cmdBytes %v", cmdBytes)
		panic(err)
	}
	util.MustReadBigEndian(bytes.NewReader(cmdBytes[:8]), &c.CmdType)
	var keyLen uint64
	util.MustReadBigEndian(bytes.NewReader(cmdBytes[8:16]), &keyLen)
	c.Key = make([]byte, keyLen)
	util.MustReadBigEndian(bytes.NewReader(cmdBytes[16:16+keyLen]), c.Key)
	var valLen uint64
	util.MustReadBigEndian(bytes.NewReader(cmdBytes[16+keyLen:24+keyLen]), &valLen)
	c.Value = make([]byte, valLen)
	util.MustReadBigEndian(bytes.NewReader(cmdBytes[24+keyLen:]), c.Value)
}

// ToBytes =>
// | CmdType (8 bytes) | key length (8 bytes) | key (...) | val length (8 bytes) | val (...) |
func (c *Cmd) ToBytes() []byte {
	buf := new(bytes.Buffer)
	util.MustWriteBigEndian(buf, c.CmdType)
	keyLen := uint64(len(c.Key))
	util.MustWriteBigEndian(buf, keyLen)
	buf.Write(c.Key)
	ValLen := uint64(len(c.Value))
	util.MustWriteBigEndian(buf, ValLen)
	buf.Write(c.Value)
	if buf.Len() != (24 + len(c.Key) + len(c.Value)) {
		err := fmt.Errorf("[Cmd][ToBytes] buffer len not match")
		panic(err)
	}
	cmdBytes := buf.Bytes()
	return cmdBytes
}

// CmdItem wraps KvStoreCmd for better use of btree.BTree and persistence
type CmdItem struct {
	Cmd
}

func (c CmdItem) Less(than btree.Item) bool {
	c2 := than.(CmdItem)
	return bytes.Compare(c.Key, c2.Key) < 0
}

type CmdList struct {
	cmds []Cmd
}

func (cl *CmdList) FromBytes(bts []byte) {
	cl.cmds = make([]Cmd, 0)
	var offset int64
	var nextCmdLen int64
	for offset < int64(len(bts)) {
		nextCmdLenBytes := bts[offset : offset+8]
		util.MustReadBigEndian(bytes.NewReader(nextCmdLenBytes), &nextCmdLen)
		offset += 8
		c := Cmd{}
		c.FromBytes(bts[offset : offset+nextCmdLen])
		cl.cmds = append(cl.cmds, c)
		offset += nextCmdLen
	}
}

func (cl *CmdList) ToBytes() []byte {
	buf := new(bytes.Buffer)
	for i := range cl.cmds {
		writeCmdToBuffer(buf, &cl.cmds[i])
	}
	return buf.Bytes()
}

func (cl *CmdList) AppendToBytes(c Cmd) []byte {
	cl.cmds = append(cl.cmds, c)
	buf := new(bytes.Buffer)
	writeCmdToBuffer(buf, &c)
	return buf.Bytes()
}

// Find finds the latest command of the key in this list
func (cl *CmdList) Find(key []byte) *Cmd {
	if len(cl.cmds) > 0 {
		for i := range cl.cmds {
			j := len(cl.cmds) - i - 1
			if bytes.Equal(cl.cmds[j].Key, key) {
				return &cl.cmds[j]
			}
		}
	}
	return nil
}

func (cl *CmdList) Clear() {
	cl.cmds = cl.cmds[:0]
}

func (cl *CmdList) ToTree() *btree.BTree {
	bt := btree.New(2)
	for i := range cl.cmds {
		cmd := cl.cmds[i]
		bt.ReplaceOrInsert(CmdItem{Cmd: cmd})
	}
	return bt
}

// cmdLen + cmdBytes
func writeCmdToBuffer(buf *bytes.Buffer, cmd *Cmd) {
	cmdBytes := cmd.ToBytes()
	cmdLen := int64(len(cmdBytes))
	util.MustWriteBigEndian(buf, cmdLen)
	buf.Write(cmdBytes)
}
