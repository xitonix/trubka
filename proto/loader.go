package proto

import (
	"errors"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
)

type Loader interface {
	Find(messageName string) (*desc.MessageDescriptor, error)
	Len() int
}

type FileLoader struct {
	files []*desc.FileDescriptor
}

func NewFileLoader(root string) (*FileLoader, error) {
	finder, err := NewFileFinder(root)
	if err != nil {
		return nil, err
	}
	files, err := finder.Ls(root)
	if err != nil {
		return nil, err
	}

	resolved, err := protoparse.ResolveFilenames([]string{root}, files...)
	if err != nil {
		return nil, err
	}

	parser := protoparse.Parser{}
	fileDescriptors, err := parser.ParseFiles(resolved...)
	if err != nil {
		return nil, err
	}

	return &FileLoader{
		files: fileDescriptors,
	}, nil
}

func (f *FileLoader) Find(messageName string) (*desc.MessageDescriptor, error) {
	for _, fd := range f.files {
		msg := fd.FindMessage(messageName)
		if msg != nil {
			return msg, nil
		}
	}
	return nil, errors.New("message not found")
}

func (f *FileLoader) Len() int {
	return len(f.files)
}
