package util

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func KeyInRange(check []byte, start []byte, end []byte) bool {
	return bytes.Compare(check, start) >= 0 && bytes.Compare(check, end) <= 0
}

func MustMkDirIfNotExist(path string) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, os.ModePerm); err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
}

func MustReadN(f *os.File, offset int64, expectedN uint64) []byte {
	bts := make([]byte, expectedN)
	n, err := f.ReadAt(bts, offset)
	if uint64(n) != expectedN {
		err = fmt.Errorf("[MustReadAt] read data at %v however bytes len not match %v vs %v", f, n, len(bts))
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	return bts
}

func MustWriteAt(f *os.File, offset int64, bts []byte) {
	n, err := f.WriteAt(bts, offset)
	if n != len(bts) {
		err = fmt.Errorf("[MustWriteAt] write data at %v however bytes len not match %v vs %v", f, n, len(bts))
		panic(err)
	}
	if err != nil {
		panic(err)
	}
}

func MustReadBigEndian(r io.Reader, data interface{}) {
	if err := binary.Read(r, binary.BigEndian, data); err != nil {
		panic(err)
	}
}

func MustWriteBigEndian(w io.Writer, data interface{}) {
	if err := binary.Write(w, binary.BigEndian, data); err != nil {
		panic(err)
	}
}

func MustMarshall(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func MustUnmarshall(data []byte, v interface{}) {
	err := json.Unmarshal(data, v)
	if err != nil {
		panic(err)
	}
}

func MustSync(f *os.File) {
	if err := f.Sync(); err != nil {
		panic(err)
	}
}
