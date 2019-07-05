package proto

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type fileFinder struct {
	root string
}

func newFileFinder(root string) (*fileFinder, error) {
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

func (f *fileFinder) ls(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(f.root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() && strings.HasSuffix(strings.ToLower(f.Name()), ".proto") {
			files = append(files, f.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
