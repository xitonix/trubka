package kafka

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kirsle/configdir"
	"github.com/peterbourgon/diskv"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
)

const (
	localOffsetRoot     = "trubka"
	offsetFileExtension = ".tpo"
)

type LocalOffsetManager struct {
	root string
	db   *diskv.Diskv
	*internal.Logger
}

func NewLocalOffsetManager(level internal.VerbosityLevel) *LocalOffsetManager {
	root := configdir.LocalConfig(localOffsetRoot)
	flatTransform := func(s string) []string { return []string{} }
	return &LocalOffsetManager{
		Logger: internal.NewLogger(level),
		root:   root,
		db: diskv.New(diskv.Options{
			BasePath:     root,
			Transform:    flatTransform,
			CacheSizeMax: 1024 * 1024,
		}),
	}
}

// ReadLocalTopicOffsets returns the locally stored offsets of the given topic for the specified environment if exists.
//
// If there is no local offsets, the method will return an empty partition-offset map.
func (l *LocalOffsetManager) ReadLocalTopicOffsets(topic string, environment string) (PartitionOffsets, error) {
	if internal.IsEmpty(environment) {
		return nil, errors.New("The environment cannot be empty")
	}
	result := make(PartitionOffsets)
	l.db.BasePath = filepath.Join(l.root, environment)
	l.Logf(internal.Verbose, "Reading the local offsets of %s topic from %s", topic, l.db.BasePath)
	val, err := l.db.Read(topic)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}

	buff := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buff)
	err = dec.Decode(&result)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to deserialize the value from local offset store for topic %s", topic)
	}

	return result, nil
}

// ListLocalOffsets lists the locally stored offsets for the the topics of all the available environments.
func (l *LocalOffsetManager) ListLocalOffsets(topicFilter *regexp.Regexp) (map[string]TopicPartitionOffset, error) {
	result := make(map[string]TopicPartitionOffset)
	root := configdir.LocalConfig("trubka")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, offsetFileExtension) {
			return nil
		}
		environment := filepath.Base(filepath.Dir(path))
		file := filepath.Base(path)
		topic := strings.TrimSuffix(file, offsetFileExtension)
		if topicFilter != nil && !topicFilter.Match([]byte(topic)) {
			return nil
		}
		po, err := l.ReadLocalTopicOffsets(file, environment)
		if err != nil {
			return err
		}
		if _, ok := result[environment]; !ok {
			result[environment] = make(TopicPartitionOffset)
		}
		result[environment][topic] = po
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
