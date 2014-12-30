package column

import (
	"encoding/binary"
	"hash/fnv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// Hash buckets

const hashBuckets = 4096

func hashBucket(key []byte) int {
	hash := fnv.New32a()
	_, _ = hash.Write(key)
	return int(hash.Sum32() % hashBuckets)
}

// Index interface
type Index interface {
	Get([]byte) ([]int64, error)
	Add([]byte, int64) error
	Close() error
}

// A Hash index type
type HashIndex struct {
	db    *leveldb.DB
	locks []sync.Mutex
}

// OpenHashIndex opens a HashIndex in dir
func OpenHashIndex(dir string) (*HashIndex, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}
	return &HashIndex{db, make([]sync.Mutex, hashBuckets)}, nil
}

func (i *HashIndex) Add(b []byte, off int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(off))

	slot := hashBucket(b)
	i.locks[slot].Lock()
	defer i.locks[slot].Unlock()

	val, err := i.db.Get(b, nil)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	return i.db.Put(b, append(val, buf...), nil)
}

func (i *HashIndex) Get(b []byte) ([]int64, error) {
	val, err := i.db.Get(b, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	res := make([]int64, 0, len(val)/8)
	for i := 0; i < len(val); i += 8 {
		off := binary.BigEndian.Uint64(val[i : i+8])
		res = append(res, int64(off))
	}
	return res, nil
}

func (i *HashIndex) Close() error {
	return i.db.Close()
}
