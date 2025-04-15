package protobuf

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"

	"github.com/xitonix/trubka/internal"
)

type fileFinder struct {
	root      string
	verbosity internal.VerbosityLevel
	logger    internal.Logger
}

func newFileFinder(verbosity internal.VerbosityLevel, root string) (*fileFinder, error) {
	if strings.HasPrefix(root, "~") {
		expanded, err := homedir.Expand(root)
		if err != nil {
			return nil, err
		}
		root = expanded
	}
	dir, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !dir.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}
	return &fileFinder{
		root:      root,
		verbosity: verbosity,
		logger:    *internal.NewLogger(verbosity),
	}, nil
}

func (f *fileFinder) ls(ctx context.Context) ([]string, error) {
	var files []string
	f.logger.Logf(internal.Verbose, "Looking for proto contracts in %s\n", f.root)
	err := filepath.Walk(f.root, func(path string, fileInfo os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err != nil {
				return err
			}
			isDir := fileInfo.IsDir()
			if isDir {
				f.logger.Logf(internal.VeryVerbose, "Loading %s\n", path)
			}
			if !isDir && strings.HasSuffix(strings.ToLower(fileInfo.Name()), ".proto") {
				f.logger.Logf(internal.Chatty, "Proto file loaded %s\n", path)
				files = append(files, path)
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (f *fileFinder) dirs(ctx context.Context) ([]string, error) {
	var dirs []string
	err := filepath.Walk(f.root, func(path string, fileInfo os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err != nil {
				return err
			}
			if fileInfo.IsDir() {
				f.logger.Logf(internal.Chatty, "Import path detected %s\n", path)
				dirs = append(dirs, path)
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return dirs, nil
}
