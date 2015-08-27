package tidbrocks

import (
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/localstore/engine"
)

type Driver struct {
}

func (driver Driver) Open(dbPath string) (engine.DB, error) {
	db := new(rocksDB)
	err := db.initialize(dbPath, nil)
	return db, err
}

func init() {
	tidb.RegisterLocalStore("rocksdb", Driver{})
}
