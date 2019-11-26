package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kirsle/configdir"
	"github.com/peterbourgon/diskv"

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

// GetOffsetFiles returns a list of all the offset files for the given environment.
func (l *LocalOffsetManager) GetOffsetFiles(environment string, topicFilter *regexp.Regexp) ([]string, error) {
	if internal.IsEmpty(environment) {
		return nil, ErrEmptyEnvironment
	}
	root := configdir.LocalConfig(localOffsetRoot, environment)
	l.Logf(internal.Verbose, "Looking for local offsets in %s", root)

	files := make([]string, 0)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, offsetFileExtension) {
			return nil
		}

		file := filepath.Base(path)
		if topicFilter != nil && !topicFilter.Match([]byte(file)) {
			return nil
		}
		l.Logf(internal.VeryVerbose, "Local offset file has been found: %s", file)
		files = append(files, path)
		return nil
	})

	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("the local offset directory could not be found at %s", root)
		}
		return nil, err
	}

	return files, nil
}

// ReadLocalTopicOffsets returns the locally stored offsets of the given topic for the specified environment if exists.
//
// If there is no local offsets, the method will return an empty partition-offset map.
func (l *LocalOffsetManager) ReadLocalTopicOffsets(topic string, environment string) (PartitionOffset, error) {
	file, err := l.setDBPath(topic, environment)
	if err != nil {
		return nil, err
	}

	stored := make(map[int32]int64)
	l.Logf(internal.VeryVerbose, "Reading the local offsets of %s topic from %s", topic, l.db.BasePath)
	val, err := l.db.Read(file)
	if err != nil {
		if os.IsNotExist(err) {
			return PartitionOffset{}, nil
		}
		return nil, err
	}

	buff := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buff)
	err = dec.Decode(&stored)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize the value from local offset store for topic %s: %w", topic, err)
	}

	return ToPartitionOffset(stored, false), nil
}

// ListLocalOffsets lists the locally stored offsets for the the topics of all the available environments.
//
// The returned map is keyed by the environment name.
func (l *LocalOffsetManager) ListLocalOffsets(topicFilter *regexp.Regexp, envFilter *regexp.Regexp) (map[string]TopicPartitionOffset, error) {
	result := make(map[string]TopicPartitionOffset)
	root := configdir.LocalConfig(localOffsetRoot)
	l.Logf(internal.Verbose, "Searching for local offsets in %s", root)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, offsetFileExtension) {
			return nil
		}
		environment := filepath.Base(filepath.Dir(path))
		if envFilter != nil && !envFilter.Match([]byte(environment)) {
			return nil
		}
		file := filepath.Base(path)
		topic := strings.TrimSuffix(file, offsetFileExtension)
		if topicFilter != nil && !topicFilter.Match([]byte(topic)) {
			l.Logf(internal.SuperVerbose, "The provided filter (%s) does not match with %s topic", topicFilter.String(), topic)
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
		l.Logf(internal.Chatty, "%d partition offset(s) found locally for %s/%s", len(po), environment, topic)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (l *LocalOffsetManager) setDBPath(topic string, environment string) (string, error) {
	if internal.IsEmpty(environment) {
		return "", ErrEmptyEnvironment
	}
	if internal.IsEmpty(topic) {
		return "", ErrEmptyTopic
	}

	l.db.BasePath = filepath.Join(l.root, environment)

	file := topic
	if !strings.HasSuffix(file, offsetFileExtension) {
		file += offsetFileExtension
	}

	return file, nil
}
