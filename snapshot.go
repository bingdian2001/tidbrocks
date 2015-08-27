package tidbrocks

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/tecbot/gorocksdb"
)

type snapshot struct {
	db *rocksDB

	snap *gorocksdb.Snapshot
	ropt *gorocksdb.ReadOptions
}

func (sp *snapshot) Release() {
	sp.ropt.Destroy()
	sp.snap.Release()
}

func (sp *snapshot) NewIterator(startKey []byte) engine.Iterator {
	it := sp.db.rkdb.NewIterator(sp.ropt)
	return &iterator{
		seekToStart: true,
		startKey:    startKey,
		iter:        it,
	}
}

func (sp *snapshot) Get(key []byte) ([]byte, error) {
	value, err := sp.db.rkdb.GetBytes(sp.ropt, key)
	return value, errors.Trace(err)
}
