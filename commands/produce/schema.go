package produce

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

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
	utcExp         *regexp.Regexp
}

func addSchemaSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters) {
	cmd := &schema{
		globalParams:   global,
		emailAddressEx: regexp.MustCompile(`(?i)email|email[-_]address|emailAddress`),
		ipAddressEx:    regexp.MustCompile(`(?i)ip[-_]address|ipAddress`),
		utcExp:         regexp.MustCompile(`(?i)utc|gmt`),
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
	fields := md.GetFields()
	for _, field := range fields {
		options := field.GetFieldOptions()
		if options != nil && options.Deprecated != nil && *options.Deprecated {
			continue
		}
		t := field.GetType()
		name := field.GetName()
		oneOff := field.GetOneOf()
		oneOffChoice = chooseOneOff(oneOff)
		if oneOffChoice != "" && name != oneOffChoice && oneOff != nil {
			continue
		}

		if t == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			if gt, set := c.getGoogleType(name, field); set {
				mp[name] = gt
				continue
			}
			parent := make(map[string]interface{})
			mp[name] = parent
			c.readSchema(parent, oneOffChoice, field.GetMessageType())
		} else {
			if c.random {
				mp[name] = c.getGeneratorFunc(field)
			} else {
				mp[name] = field.GetDefaultValue()
			}
		}
	}
}

func chooseOneOff(oneOff *desc.OneOfDescriptor) string {
	if oneOff == nil {
		return ""
	}
	choices := oneOff.GetChoices()
	for _, choice := range choices {
		options := choice.GetFieldOptions()
		if options != nil && options.Deprecated != nil && *options.Deprecated {
			continue
		}
		return choice.GetName()
	}
	return ""
}

func (c *schema) getGoogleType(name string, field *desc.FieldDescriptor) (value string, set bool) {
	ft := field.GetMessageType()
	if ft == nil {
		return "", false
	}

	switch ft.GetFullyQualifiedName() {
	case "google.protobuf.Timestamp":
		set = true
		if c.random {
			if c.utcExp.MatchString(name) {
				value = fmt.Sprintf("Now('%s','UTC')", time.RFC3339)
			} else {
				value = fmt.Sprintf("Now('%s')", time.RFC3339)
			}
		}
	case "google.protobuf.Duration":
		set = true
		if c.random {
			value = "FloatS(1,10,3)s"
		}
	default:
		value = ""
		set = false
	}
	return
}

func (c *schema) getGeneratorFunc(field *desc.FieldDescriptor) interface{} {
	name := strings.ToLower(field.GetName())
	t := field.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return c.getStringFunc(name)
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return "Float(##.##)"
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return "Float(##.##)"
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		return "Int(########)"
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		return "Int(########)"
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		return "Int(#####)"
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return "Int(########)"
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return "Int(#####)"
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return "Bool()"
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return "B64(???????)"
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		return "Int(#####)"
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		values := field.GetEnumType().GetValues()
		var min, max int32
		for _, v := range values {
			options := v.GetEnumValueOptions()
			if options != nil && options.Deprecated != nil && *options.Deprecated {
				continue
			}
			num := v.GetNumber()
			if num < min {
				min = num
			}
			if num > max {
				max = num
			}
		}
		return fmt.Sprintf("Int(%d,%d)", min, max)
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return "Int(#####)"
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return "Int(########)"
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		return "Int(#####)"
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		return "Int(########)"
	default:
		return field.GetDefaultValue()
	}
}

func (c *schema) getStringFunc(name string) interface{} {
	if c.emailAddressEx.MatchString(name) {
		return "Email()"
	}
	if c.ipAddressEx.MatchString(name) {
		return "IP(v4)"
	}
	return "Str(?????)"
}
