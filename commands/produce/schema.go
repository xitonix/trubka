package produce

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/protobuf"
)

type schema struct {
	globalParams *commands.GlobalParameters
	proto        string
	protoRoot    string
}

func addSchemaSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters) {
	cmd := &schema{
		globalParams: global,
	}
	c := parent.Command("schema", "Produces the JSON representation of the given proto message. The produced schema can be used to publish to Kafka.").Action(cmd.run)
	c.Arg("proto", "The fully qualified name of the proto message to generate the JSON schema of.").Required().StringVar(&cmd.proto)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
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
	readSchema(mp, "", msg.GetMessageDescriptor())
	b, err := json.MarshalIndent(mp, " ", "   ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func readSchema(mp map[string]interface{}, oneOffChoice string, md *desc.MessageDescriptor) {

	for _, field := range md.GetFields() {
		options := field.GetFieldOptions()
		if options != nil && options.Deprecated != nil && *options.Deprecated {
			continue
		}
		t := field.GetType()
		if t == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			name := field.GetName()
			oneOffChoice = chooseOneOff(field.GetOneOf())
			if oneOffChoice != "" && name != oneOffChoice && field.GetOneOf() != nil {
				continue
			}
			parent := make(map[string]interface{})
			mp[field.GetName()] = parent
			readSchema(parent, oneOffChoice, field.GetMessageType())
		} else {
			options := field.GetFieldOptions()
			if options != nil && options.Deprecated != nil && *options.Deprecated {
				continue
			}
			mp[field.GetName()] = field.GetDefaultValue()
		}
	}
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
