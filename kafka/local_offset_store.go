package kafka

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"go.xitonix.io/trubka/internal"
)

type localOffsetStore struct {
	db      *badger.DB
	printer internal.Printer

	mux sync.Mutex
	m   map[string]map[int32]int64
}

func newLocalOffsetStore(printer internal.Printer, filePath string) (*localOffsetStore, error) {
	db, err := badger.Open(badger.DefaultOptions(filePath).WithLogger(nil))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create the local offset file")
	}
	return &localOffsetStore{
		db:      db,
		m:       make(map[string]map[int32]int64),
		printer: printer,
	}, nil
}

func (s *localOffsetStore) Store(topic string, partition int32, offset int64) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	_, ok := s.m[topic]
	if !ok {
		s.m[topic] = make(map[int32]int64)
	}
	s.m[topic][partition] = offset
	buff := bytes.Buffer{}
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(s.m[topic])
	if err != nil {
		return errors.Wrapf(err, "Failed to serialise the offsets for topic %s", topic)
	}
	if buff.Len() > 0 {
		s.printer.Writef(internal.SuperVerbose, "Storing partition %d offset %d for topic %s\n", partition, offset, topic)
		err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(topic), buff.Bytes())
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to store the offsets for topic %s", topic)
		}
	}
	return nil
}

func (s *localOffsetStore) Query(topic string) (map[int32]int64, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if offsets, ok := s.m[topic]; ok {
		return offsets, nil
	}
	return s.queryDB(topic)
}

func (s *localOffsetStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.printer.Writeln(internal.SuperVerbose, "Synchronising the offset store.")
	if err := s.db.Sync(); err != nil {
		s.printer.Writef(internal.Quiet, "Failed to sync the local offset file: %s.\n", err)
	}
	err := s.db.Close()
	if err == nil {
		s.printer.Writeln(internal.SuperVerbose, "The offset store has been closed successfully.")
	}
	return err
}

func (s *localOffsetStore) queryDB(topic string) (map[int32]int64, error) {
	offsets := make(map[int32]int64)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(topic))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		err = item.Value(func(val []byte) error {
			buff := bytes.NewBuffer(val)
			dec := gob.NewDecoder(buff)
			err := dec.Decode(offsets)
			if err != nil {
				return errors.Wrapf(err, "Failed to deserialize the value from local offset store for topic %s", topic)
			}
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to read the value from local offset store for topic %s", topic)
		}
		s.m[topic] = offsets
		return nil
	})
	if err != nil {
		return nil, err
	}
	return offsets, nil
}
