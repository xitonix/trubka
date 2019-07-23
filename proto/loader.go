package proto

import (
	"path/filepath"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

type Loader interface {
	Load(messageName string) (*dynamic.Message, error)
	List() []string
}

const protoExtension = ".proto"

type FileLoader struct {
	files     []*desc.FileDescriptor
	prefix    string
	hasPrefix bool

	mux   sync.Mutex
	cache map[string]*desc.MessageDescriptor
}

func NewFileLoader(root string, prefix string, files ...string) (*FileLoader, error) {
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
			if !strings.HasSuffix(strings.ToLower(f), protoExtension) {
				f += protoExtension
			}
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

	prefix = strings.TrimSpace(prefix)
	return &FileLoader{
		files:     fileDescriptors,
		cache:     make(map[string]*desc.MessageDescriptor),
		prefix:    prefix,
		hasPrefix: len(prefix) > 0,
	}, nil
}

func (f *FileLoader) Load(messageName string) (*dynamic.Message, error) {
	if f.hasPrefix && !strings.HasPrefix(messageName, f.prefix) {
		messageName = f.prefix + messageName
	}
	if md, ok := f.cache[messageName]; ok {
		return dynamic.NewMessage(md), nil
	}
	for _, fd := range f.files {
		md := fd.FindMessage(messageName)
		if md != nil {
			f.mux.Lock()
			f.cache[messageName] = md
			f.mux.Unlock()
			return dynamic.NewMessage(md), nil
		}
	}
	return nil, errors.Errorf("%s not found. Make sure you use the fully qualified name of the message", messageName)
}

func (f *FileLoader) List() []string {
	result := make([]string, 0)
	for _, fd := range f.files {
		messages := fd.GetMessageTypes()
		for _, msg := range messages {
			result = append(result, msg.GetFullyQualifiedName())
		}
	}
	return result
}
