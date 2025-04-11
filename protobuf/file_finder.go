package protobuf

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"

	"github.com/xitonix/trubka/internal"
)

type fileFinder struct {
	root      string
	verbosity internal.VerbosityLevel
	logger    log.Logger
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
		logger:    *log.New(os.Stderr, "", 0),
	}, nil
}

func (f *fileFinder) ls(ctx context.Context) ([]string, error) {
	var files []string
	if f.verbosity >= internal.Verbose {
		log.Printf("Looking for proto contracts in %s\n", f.root)
	}
	err := filepath.Walk(f.root, func(path string, fileInfo os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err != nil {
				return err
			}
			isDir := fileInfo.IsDir()
			if f.verbosity >= internal.VeryVerbose && isDir {
				log.Printf("Loading %s\n", path)
			}
			if !isDir && strings.HasSuffix(strings.ToLower(fileInfo.Name()), ".proto") {
				if f.verbosity >= internal.Chatty {
					log.Printf("Proto file loaded %s\n", path)
				}
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
				if f.verbosity >= internal.Chatty {
					log.Printf("Import path detected %s\n", path)
				}
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
