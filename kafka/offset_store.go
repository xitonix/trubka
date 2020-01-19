package kafka

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kirsle/configdir"
	"github.com/peterbourgon/diskv"

	"github.com/xitonix/trubka/internal"
)

type progress struct {
	topic     string
	partition int32
	offset    int64
}

type offsetStore struct {
	db          *diskv.Diskv
	printer     internal.Printer
	wg          sync.WaitGroup
	writeErrors chan error
	in          chan *progress
	checksum    map[string]interface{}
}

func newOffsetStore(printer internal.Printer, environment string) (*offsetStore, error) {
	environment = strings.ToLower(strings.TrimSpace(environment))
	if len(environment) == 0 {
		return nil, errors.New("empty environment value is not acceptable")
	}
	root := configdir.LocalConfig(localOffsetRoot, environment)
	err := configdir.MakePath(root)
	if err != nil {
		return nil, fmt.Errorf("failed to create the application cache folder: %w", err)
	}
	printer.Infof(internal.Verbose, "Initialising local offset store at %s", root)

	flatTransform := func(s string) []string { return []string{} }

	db := diskv.New(diskv.Options{
		BasePath:     root,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})

	return &offsetStore{
		db:          db,
		printer:     printer,
		writeErrors: make(chan error),
		in:          make(chan *progress, 500),
		checksum:    make(map[string]interface{}),
	}, nil
}

func (s *offsetStore) start(loaded TopicPartitionOffset) {
	s.wg.Add(1)
	ticker := time.NewTicker(3 * time.Second)
	offsets := make(TopicPartitionOffset)
	for t, lpo := range loaded {
		partOffsets := make(PartitionOffset)
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
					s.printer.Info(internal.Verbose, "Flushing the offsets to disk.")
					s.writeOffsetsToDisk(offsets)
					return
				}
				_, ok := offsets[p.topic]
				if !ok {
					offsets[p.topic] = make(PartitionOffset)
				}
				offsets[p.topic][p.partition] = Offset{Current: p.offset}
			}
		}
	}()
}

// Store saves the topic offset to the local disk.
func (s *offsetStore) Store(topic string, partition int32, offset int64) error {
	s.in <- &progress{
		topic:     topic,
		partition: partition,
		offset:    offset,
	}
	return nil
}

// Returns the channel on which the write errors will be received.
// You must listen to this channel to avoid deadlock.
func (s *offsetStore) errors() <-chan error {
	return s.writeErrors
}

func (s *offsetStore) close() {
	if s == nil || s.db == nil {
		return
	}
	s.printer.Info(internal.SuperVerbose, "Closing the offset store.")
	close(s.in)
	s.wg.Wait()
	close(s.writeErrors)
	s.printer.Info(internal.SuperVerbose, "The offset store has been closed successfully.")
}

func (s *offsetStore) writeOffsetsToDisk(topicPartitionOffsets TopicPartitionOffset) {
	for topic, partitionOffsets := range topicPartitionOffsets {
		cs, buff, err := partitionOffsets.marshal()
		if err != nil {
			s.writeErrors <- fmt.Errorf("failed to serialise the offsets of topic %s: %w", topic, err)
			return
		}
		if cs == "" {
			return
		}
		if _, ok := s.checksum[cs]; ok {
			return
		}
		s.checksum[cs] = nil
		s.printer.Infof(internal.SuperVerbose, "Writing the offset(s) of topic %s to the disk.", topic)
		for p, offset := range partitionOffsets {
			if offset.Current >= 0 {
				s.printer.Logf(internal.Chatty, " P%02d: %d", p, offset.Current)
			}
		}
		err = s.db.Write(topic+offsetFileExtension, buff)
		if err != nil {
			s.writeErrors <- fmt.Errorf("failed to write the offsets of topic %s to the disk %s: %w", topic, cs, err)
		}
	}
}
