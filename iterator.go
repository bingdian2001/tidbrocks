package tidbrocks

import "github.com/tecbot/gorocksdb"

type iterator struct {
	seekToStart bool
	startKey    []byte
	iter        *gorocksdb.Iterator
}

func (it *iterator) Release() {
	it.iter.Close()
}

func (it *iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *iterator) Next() bool {
	if it.seekToStart {
		it.iter.Seek(it.startKey)
		it.seekToStart = false
		return it.Valid()
	}

	it.iter.Next()
	return it.Valid()
}

func (it *iterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *iterator) Value() []byte {
	return it.iter.Value().Data()
}
