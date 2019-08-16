package kafka

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
	"time"

	"github.com/peterbourgon/diskv"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
)

type progress struct {
	topic     string
	partition int32
	offset    int64
}

type localOffsetStore struct {
	db          *diskv.Diskv
	printer     internal.Printer
	wg          sync.WaitGroup
	writeErrors chan error
	in          chan *progress
	checksum    map[string]interface{}
}

func newLocalOffsetStore(printer internal.Printer, base string) (*localOffsetStore, error) {
	printer.Logf(internal.Verbose, "Initialising local offset store at %s", base)

	flatTransform := func(s string) []string { return []string{} }

	db := diskv.New(diskv.Options{
		BasePath:     base,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})

	return &localOffsetStore{
		db:          db,
		printer:     printer,
		writeErrors: make(chan error),
		in:          make(chan *progress, 10),
		checksum:    make(map[string]interface{}),
	}, nil
}

func (s *localOffsetStore) start(loaded map[string]PartitionOffsets) {
	s.wg.Add(1)
	ticker := time.NewTicker(3 * time.Second)
	offsets := make(map[string]PartitionOffsets)
	for t, lpo := range loaded {
		partOffsets := make(PartitionOffsets)
		lpo.copyTo(partOffsets)
		offsets[t] = partOffsets
	}
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ticker.C:
				s.writeOffsetsToDisk(offsets)
			case p, more := <-s.in:
				if !more {
					ticker.Stop()
					s.printer.Log(internal.Verbose, "Flushing the offsets to disk.")
					s.writeOffsetsToDisk(offsets)
					return
				}
				_, ok := offsets[p.topic]
				if !ok {
					offsets[p.topic] = make(PartitionOffsets)
				}
				offsets[p.topic][p.partition] = p.offset
			}
		}
	}()
}

// Store saves the topic offset to the local disk.
func (s *localOffsetStore) Store(topic string, partition int32, offset int64) error {
	s.in <- &progress{
		topic:     topic,
		partition: partition,
		offset:    offset,
	}
	return nil
}

// Query loads the offsets of all the available partitions from the local disk.
func (s *localOffsetStore) Query(topic string) (PartitionOffsets, error) {
	offsets := make(PartitionOffsets)
	val, err := s.db.Read(topic)
	if err != nil {
		if os.IsNotExist(err) {
			return offsets, nil
		}
		return nil, err
	}

	buff := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buff)
	err = dec.Decode(&offsets)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to deserialize the value from local offset store for topic %s", topic)
	}
	return offsets, nil
}

// Returns the channel on which the write errors will be received.
// You must listen to this channel to avoid deadlock.
func (s *localOffsetStore) errors() <-chan error {
	return s.writeErrors
}

func (s *localOffsetStore) close() {
	if s == nil || s.db == nil {
		return
	}
	s.printer.Log(internal.SuperVerbose, "Closing the offset store.")
	close(s.in)
	s.wg.Wait()
	close(s.writeErrors)
	s.printer.Log(internal.SuperVerbose, "The offset store has been closed successfully.")
}

func (s *localOffsetStore) writeOffsetsToDisk(offsets map[string]PartitionOffsets) {
	for topic, offsets := range offsets {
		cs, buff, err := offsets.marshal()
		if err != nil {
			s.writeErrors <- errors.Wrapf(err, "Failed to serialise the offsets of topic %s", topic)
			return
		}
		if cs == "" {
			return
		}
		if _, ok := s.checksum[cs]; ok {
			return
		}
		s.checksum[cs] = nil
		s.printer.Logf(internal.SuperVerbose, "Writing the offset(s) of topic %s to the disk.", topic)
		for p, o := range offsets {
			if o >= 0 {
				s.printer.Logf(internal.Chatty, " P%02d: %d", p, o)
			}
		}
		err = s.db.Write(topic, buff)
		if err != nil {
			s.writeErrors <- errors.Wrapf(err, "Failed to write the offsets of topic %s to the disk %s", topic, cs)
		}
	}
}
