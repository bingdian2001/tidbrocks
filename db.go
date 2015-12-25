package tidbrocks

import (
	"os"

	"github.com/c4pt0r/gorocksdb"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/localstore/engine"
)

type rocksDB struct {
	path string
	rkdb *gorocksdb.DB
	opts *gorocksdb.Options
	ropt *gorocksdb.ReadOptions
	wopt *gorocksdb.WriteOptions

	env   *gorocksdb.Env
	topts *gorocksdb.BlockBasedTableOptions
	cache *gorocksdb.Cache

	snapshotFillCache bool
}

func (db *rocksDB) initialize(path string, conf *config) error {
	if conf == nil {
		conf = newDefaultConfig()
	}

	// Create path if not exists first
	if err := os.MkdirAll(path, 0700); err != nil {
		return errors.Trace(err)
	}

	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(false)

	opts.SetCompression(gorocksdb.CompressionType(conf.CompressionType))
	opts.SetWriteBufferSize(conf.WriteBufferSize)
	opts.SetMaxOpenFiles(conf.MaxOpenFiles)
	opts.SetNumLevels(conf.NumLevels)

	opts.SetMaxWriteBufferNumber(conf.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(conf.MinWriteBufferNumberToMerge)
	opts.SetLevel0FileNumCompactionTrigger(conf.Level0FileNumCompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(conf.Level0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(conf.Level0StopWritesTrigger)
	opts.SetTargetFileSizeBase(uint64(conf.TargetFileSizeBase))
	opts.SetTargetFileSizeMultiplier(conf.TargetFileSizeMultiplier)
	opts.SetMaxBytesForLevelBase(uint64(conf.MaxBytesForLevelBase))
	opts.SetMaxBytesForLevelMultiplier(conf.MaxBytesForLevelMultiplier)

	opts.SetDisableAutoCompactions(conf.DisableAutoCompactions)
	opts.SetDisableDataSync(conf.DisableDataSync)
	opts.SetUseFsync(conf.UseFsync)
	opts.SetMaxBackgroundCompactions(conf.MaxBackgroundCompactions)
	opts.SetMaxBackgroundFlushes(conf.MaxBackgroundFlushes)
	opts.SetAllowOsBuffer(conf.AllowOSBuffer)

	topts := gorocksdb.NewDefaultBlockBasedTableOptions()
	topts.SetBlockSize(conf.BlockSize)

	cache := gorocksdb.NewLRUCache(conf.CacheSize)
	topts.SetBlockCache(cache)

	topts.SetFilterPolicy(gorocksdb.NewBloomFilter(conf.BloomFilterSize))
	opts.SetBlockBasedTableFactory(topts)

	env := gorocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(conf.BackgroundThreads)
	env.SetHighPriorityBackgroundThreads(conf.HighPriorityBackgroundThreads)
	opts.SetEnv(env)

	db.path = path
	db.opts = opts
	db.ropt = gorocksdb.NewDefaultReadOptions()
	db.wopt = gorocksdb.NewDefaultWriteOptions()
	db.env = env
	db.topts = topts
	db.cache = cache
	db.snapshotFillCache = conf.SnapshotFillCache

	var err error
	if db.rkdb, err = gorocksdb.OpenDb(db.opts, db.path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (db *rocksDB) Close() error {
	db.rkdb.Close()

	db.opts.Destroy()
	db.ropt.Destroy()
	db.wopt.Destroy()
	db.topts.Destroy()
	db.env.Destroy()
	db.cache.Destroy()

	return nil
}

/*
func (db *rocksDB) GetSnapshot() (engine.Snapshot, error) {
	snap := db.rkdb.NewSnapshot()

	ropt := gorocksdb.NewDefaultReadOptions()
	ropt.SetFillCache(db.snapshotFillCache)
	ropt.SetSnapshot(snap)

	return &snapshot{
		db:   db,
		snap: snap,
		ropt: ropt,
	}, nil
}
*/

type rocksBatch struct {
	*gorocksdb.WriteBatch
}

func (batch *rocksBatch) Len() int {
	return batch.Count()
}

func (db *rocksDB) NewBatch() engine.Batch {
	return &rocksBatch{WriteBatch: gorocksdb.NewWriteBatch()}
}

func (db *rocksDB) Get(key []byte) ([]byte, error) {
	value, err := db.rkdb.GetBytes(db.ropt, key)
	return value, errors.Trace(err)
}

func cloneBytes(s []byte) []byte {
	return append([]byte(nil), s...)
}

func (db *rocksDB) Seek(key []byte) ([]byte, []byte, error) {
	it := db.rkdb.NewIterator(db.ropt)
	defer it.Close()
	it.Seek(key)
	if it.Valid() {
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()
		return cloneBytes(key.Data()), cloneBytes(value.Data()), nil
	}

	err := engine.ErrNotFound
	if it.Err() != nil {
		err = it.Err()
	}

	return nil, nil, err
}

func (db *rocksDB) MultiSeek(keys [][]byte) []*engine.MSeekResult {
	it := db.rkdb.NewIterator(db.ropt)
	defer it.Close()
	keys, vals := db.rkdb.MultiSeek(it, keys)

	results := make([]*engine.MSeekResult, 0, len(keys))
	for i, key := range keys {
		if len(vals[i]) == 1 && vals[i][0] == '\x00' {
			err := engine.ErrNotFound
			if it.Err() != nil {
				err = it.Err()
			}
			results = append(results, &engine.MSeekResult{Err: err})

		} else {
			key := key
			value := vals[i]
			results = append(results, &engine.MSeekResult{
				Key:   cloneBytes(key),
				Value: cloneBytes(value),
			})
		}
	}

	return results
}

func (db *rocksDB) Commit(bt engine.Batch) error {
	batch, ok := bt.(*rocksBatch)
	if !ok {
		return errors.Errorf("invalid batch type %T", bt)
	}

	defer batch.Destroy()

	return errors.Trace(db.rkdb.Write(db.wopt, batch.WriteBatch))
}
