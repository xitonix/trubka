package protobuf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
)

type fileFinder struct {
	root string
}

func newFileFinder(root string) (*fileFinder, error) {
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
		root: root,
	}, nil
}

func (f *fileFinder) ls() ([]string, error) {
	var files []string
	err := filepath.Walk(f.root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() && strings.HasSuffix(strings.ToLower(f.Name()), ".proto") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (f *fileFinder) dirs() ([]string, error) {
	var dirs []string
	err := filepath.Walk(f.root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dirs, nil
}
