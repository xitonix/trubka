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

func (l *LocalOffsetManager) GetOffsetFileOrRoot(environment string, topic string) (string, error) {
	if internal.IsEmpty(environment) {
		return "", ErrEmptyEnvironment
	}

	singleTopicMode := !internal.IsEmpty(topic) && !strings.EqualFold(topic, "all")
	offsetPath := configdir.LocalConfig(localOffsetRoot, environment)
	if singleTopicMode {
		offsetPath = filepath.Join(offsetPath, topic+offsetFileExtension)
	}
	_, err := os.Stat(offsetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no consumer offset has been found in %s", offsetPath)
		}
		return "", fmt.Errorf("failed to access the requested local offset : %w", err)
	}
	return offsetPath, nil
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

// List lists all the environments and the topics stored locally
func (l *LocalOffsetManager) List(topicFilter *regexp.Regexp, envFilter *regexp.Regexp) (map[string][]string, error) {
	result := make(map[string][]string)
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
			l.Logf(internal.SuperVerbose, "The provided environment filter (%s) does not match with %s environment", envFilter.String(), environment)
			return nil
		}
		file := filepath.Base(path)
		topic := strings.TrimSuffix(file, offsetFileExtension)
		if topicFilter != nil && !topicFilter.Match([]byte(topic)) {
			l.Logf(internal.SuperVerbose, "The provided topic filter (%s) does not match with %s topic", topicFilter.String(), topic)
			return nil
		}
		if _, ok := result[environment]; !ok {
			result[environment] = make([]string, 0)
		}
		result[environment] = append(result[environment], topic)
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
