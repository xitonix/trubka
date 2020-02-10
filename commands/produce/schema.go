package produce

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/protobuf"
)

type schema struct {
	globalParams   *commands.GlobalParameters
	proto          string
	protoRoot      string
	random         bool
	emailAddressEx *regexp.Regexp
	ipAddressEx    *regexp.Regexp
}

func addSchemaSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters) {
	cmd := &schema{
		globalParams:   global,
		emailAddressEx: regexp.MustCompile(`(?i)email|email[-_]address|emailAddress`),
		ipAddressEx:    regexp.MustCompile(`(?i)ip[-_]address|ipAddress`),
	}
	c := parent.Command("schema", "Produces the JSON representation of the given proto message. The produced schema can be used to publish to Kafka.").Action(cmd.run)
	c.Arg("proto", "The fully qualified name of the proto message to generate the JSON schema of.").Required().StringVar(&cmd.proto)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
	c.Flag("random-generators", "Use random generator functions for each field instead of default values.").
		Short('g').
		BoolVar(&cmd.random)
}

func (c *schema) run(_ *kingpin.ParseContext) error {
	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	err = loader.Load(c.proto)
	if err != nil {
		return err
	}

	msg, err := loader.Get(c.proto)
	if err != nil {
		return err
	}
	mp := make(map[string]interface{})
	c.readSchema(mp, "", msg.GetMessageDescriptor())
	b, err := json.MarshalIndent(mp, " ", "   ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func (c *schema) readSchema(mp map[string]interface{}, oneOffChoice string, md *desc.MessageDescriptor) {
	for _, field := range md.GetFields() {
		options := field.GetFieldOptions()
		if options != nil && options.Deprecated != nil && *options.Deprecated {
			continue
		}
		t := field.GetType()
		name := field.GetName()
		if t == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			oneOffChoice = chooseOneOff(field.GetOneOf())
			if oneOffChoice != "" && name != oneOffChoice && field.GetOneOf() != nil {
				continue
			}
			parent := make(map[string]interface{})
			mp[field.GetName()] = parent
			c.readSchema(parent, oneOffChoice, field.GetMessageType())
		} else {
			options := field.GetFieldOptions()
			if options != nil && options.Deprecated != nil && *options.Deprecated {
				continue
			}
			defaultValue := field.GetDefaultValue()
			if c.random {
				mp[name] = c.getGeneratorFunc(name, t, defaultValue)
			} else {
				mp[name] = defaultValue
			}
		}
	}
}

func (c *schema) getGeneratorFunc(name string, t descriptor.FieldDescriptorProto_Type, fallback interface{}) interface{} {
	name = strings.ToLower(name)
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return c.getStringFunc(name)
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return "[Double]"
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return "[Float]"
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		return "[Int64]"
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		return "[UInt64]"
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		return "[Int64]"
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return "[FUInt64]"
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return "[FUInt32]"
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return "[Bool]"
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return "[Bytes]"
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		return "[UInt32]"
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return "[UInt8]"
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return "[FInt32]"
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return "[FInt64]"
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		return "[FInt32]"
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		return "[FInt64]"
	default:
		return fallback
	}
}

func (c *schema) getStringFunc(name string) interface{} {
	if c.emailAddressEx.MatchString(name) {
		return "[Email]"
	}
	if c.ipAddressEx.MatchString(name) {
		return "[IPV4]"
	}
	return "[Text]"
}

func chooseOneOff(parent *desc.OneOfDescriptor) string {
	if parent == nil {
		return ""
	}
	choices := parent.GetChoices()
	for _, choice := range choices {
		options := choice.GetFieldOptions()
		if options != nil && options.Deprecated != nil && *options.Deprecated {
			continue
		}
		return choice.GetName()
	}
	return ""
}
