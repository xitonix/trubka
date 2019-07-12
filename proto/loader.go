package proto

import (
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

type Loader interface {
	Load(messageName string) (proto.Message, error)
}

type FileLoader struct {
	files []*desc.FileDescriptor
}

func NewFileLoader(root string, files ...string) (*FileLoader, error) {
	finder, err := newFileFinder(root)
	if err != nil {
		return nil, err
	}

	// We will load all the proto files
	if len(files) == 0 {
		files, err = finder.ls()
		if err != nil {
			return nil, errors.Wrap(err, "failed to load the proto files")
		}
	} else {
		for i, f := range files {
			if !filepath.IsAbs(f) {
				files[i] = filepath.Join(root, f)
			}
		}
	}

	importPaths, err := finder.dirs()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load the import paths")
	}

	if len(files) == 0 {
		return nil, errors.Errorf("no protocol buffer (*.proto) files found in %s", root)
	}

	resolved, err := protoparse.ResolveFilenames(importPaths, files...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve the protocol buffer (*.proto) files")
	}

	parser := protoparse.Parser{
		ImportPaths: importPaths,
	}

	fileDescriptors, err := parser.ParseFiles(resolved...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse the protocol buffer (*.proto) files")
	}

	return &FileLoader{
		files: fileDescriptors,
	}, nil
}

func (f *FileLoader) Load(messageName string) (proto.Message, error) {
	for _, fd := range f.files {
		md := fd.FindMessage(messageName)
		if md != nil {
			msg := dynamic.NewMessage(md)
			return msg, nil
		}
	}
	return nil, errors.Errorf("%s not found. Make sure you use the fully qualified name of the message", messageName)
}
