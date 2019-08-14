package protobuf

import (
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
)

// Loader the interface to load and list the protocol buffer message types.
type Loader interface {
	Load(messageName string) error
	Get(messageName string) (*dynamic.Message, error)
	List(filter string) ([]string, error)
}

const protoExtension = ".proto"

// FileLoader is an implementation of Loader interface to load the proto files from the disk.
type FileLoader struct {
	files     []*desc.FileDescriptor
	prefix    string
	hasPrefix bool

	mux   sync.Mutex
	cache map[string]*desc.MessageDescriptor
}

// NewFileLoader creates a new instance of local file loader.
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
		ImportPaths:           importPaths,
		IncludeSourceCodeInfo: true,
	}

	// Temporary workaround for: https://github.com/jhump/protoreflect/issues/254
	if runtime.GOOS == "windows" {
		for i := range resolved {
			resolved[i] = filepath.ToSlash(resolved[i])
		}
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

// Load loads the specified message type into the local cache.
//
// The input parameter must be the fully qualified name of the message type.
// The method will return an error if the specified message type does not exist in the path.
//
// Calling load is not thread safe.
func (f *FileLoader) Load(messageName string) error {
	messageName = f.prefixIfNeeded(messageName)
	_, ok := f.cache[messageName]
	if ok {
		return nil
	}
	for _, fd := range f.files {
		md := fd.FindMessage(messageName)
		if md != nil {
			f.cache[messageName] = md
			return nil
		}
	}
	return errors.Errorf("%s not found. Make sure you use the fully qualified name of the message", messageName)
}

// Get creates a new instance of the specified protocol buffer message.
//
// The input parameter must be the fully qualified name of the message type.
// The method will return an error if the specified message type does not exist in the path.
func (f *FileLoader) Get(messageName string) (*dynamic.Message, error) {
	messageName = f.prefixIfNeeded(messageName)
	if md, ok := f.cache[messageName]; ok {
		return dynamic.NewMessage(md), nil
	}
	return nil, errors.Errorf("%s not found. Make sure you Load the message first.", messageName)
}

// List returns a list of all the protocol buffer messages exist in the path.
func (f *FileLoader) List(filter string) ([]string, error) {
	var search *regexp.Regexp
	if !internal.IsEmpty(filter) {
		s, err := regexp.Compile(filter)
		if err != nil {
			return nil, errors.Wrap(err, "invalid type filter regular expression")
		}
		search = s
	}
	result := make([]string, 0)
	for _, fd := range f.files {
		messages := fd.GetMessageTypes()
		for _, msg := range messages {
			name := msg.GetFullyQualifiedName()
			if search == nil {
				result = append(result, name)
				continue
			}
			if search.Match([]byte(name)) {
				result = append(result, name)
			}
		}
	}
	return result, nil
}

func (f *FileLoader) prefixIfNeeded(messageName string) string {
	if f.hasPrefix && !strings.HasPrefix(messageName, f.prefix) {
		return f.prefix + messageName
	}
	return messageName
}
