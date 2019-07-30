package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"go.xitonix.io/trubka/internal"
)

type progress struct {
	topic     string
	partition int32
	offset    int64
}

type localOffsetStore struct {
	db          *badger.DB
	printer     internal.Printer
	wg          sync.WaitGroup
	writeErrors chan error
	in          chan *progress

	offsets map[string]map[int32]int64
}

func newLocalOffsetStore(printer internal.Printer, filePath string) (*localOffsetStore, error) {
	printer.Logf(internal.Verbose, "Initialising local offset store at %s", filePath)
	ops := badger.DefaultOptions(filePath).WithLogger(nil)
	db, err := badger.Open(ops)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create the local offset file")
	}

	return &localOffsetStore{
		db:          db,
		printer:     printer,
		writeErrors: make(chan error),
		in:          make(chan *progress, 100),
		offsets:     make(map[string]map[int32]int64),
	}, nil
}

func (s *localOffsetStore) Start() {
	s.wg.Add(1)
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ticker.C:
				s.writeOffsetsToDisk()
			case p, more := <-s.in:
				if !more {
					ticker.Stop()
					s.printer.Log(internal.Verbose, "Flushing the offset store")
					s.writeOffsetsToDisk()
					return
				}
				_, ok := s.offsets[p.topic]
				if !ok {
					s.offsets[p.topic] = make(map[int32]int64)
				}
				s.offsets[p.topic][p.partition] = p.offset
			}
		}
	}()
}

func (s *localOffsetStore) writeOffsetsToDisk() {
	for topic, offsets := range s.offsets {
		buff := bytes.Buffer{}
		enc := gob.NewEncoder(&buff)
		err := enc.Encode(offsets)
		if err != nil {
			s.writeErrors <- errors.Wrapf(err, "Failed to serialise the offsets of topic %s", topic)
		}
		if buff.Len() > 0 {
			var offsetsString string
			if s.printer.Level() == internal.SuperVerbose {
				offsetsString = fmt.Sprintf(" %v", offsets)
			}
			s.printer.Logf(internal.VeryVerbose, "Writing the offsets%v of topic %s to the disk", offsetsString, topic)
			err := s.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(topic), buff.Bytes())
			})
			if err != nil {
				s.writeErrors <- errors.Wrapf(err, "Failed to write the offsets %v of topic %s to the disk", offsets, topic)
			}
		}
	}
}

// Errors returns the channel on which the write errors will be received.
//
// You must listen to this channel to avoid deadlock.
func (s *localOffsetStore) Errors() <-chan error {
	return s.writeErrors
}

func (s *localOffsetStore) Store(topic string, partition int32, offset int64) error {
	s.in <- &progress{
		topic:     topic,
		partition: partition,
		offset:    offset,
	}
	return nil
}

func (s *localOffsetStore) Query(topic string) (map[int32]int64, error) {
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
			err := dec.Decode(&offsets)
			if err != nil {
				return errors.Wrapf(err, "Failed to deserialize the value from local offset store for topic %s", topic)
			}
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to read the value from local offset store for topic %s", topic)
		}
		s.offsets[topic] = offsets
		return nil
	})
	if err != nil {
		return nil, err
	}
	return offsets, nil
}

func (s *localOffsetStore) Close() {
	if s == nil || s.db == nil {
		return
	}
	s.printer.Log(internal.SuperVerbose, "Closing the offset store.")
	close(s.in)
	s.wg.Wait()
	close(s.writeErrors)
	s.printer.Log(internal.SuperVerbose, "Synchronising the offset store.")
	if err := s.db.Sync(); err != nil {
		s.printer.Logf(internal.Forced, "Failed to sync the local offset file: %s.", err)
	}
	err := s.db.Close()
	if err == nil {
		s.printer.Log(internal.SuperVerbose, "The offset store has been closed successfully.")
	}
}
