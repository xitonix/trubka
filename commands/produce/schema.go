package produce

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/jsonpb"
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

	marshaller := jsonpb.Marshaler{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
		OrigName:     false,
		AnyResolver:  nil,
	}

	msg, err := loader.Get(c.proto)
	if err != nil {
		return err
	}

	b, err := msg.MarshalJSONPB(&marshaller)
	if err != nil {
		return err
	}

	fmt.Println(strings.ReplaceAll(string(b), "null", "{}"))

	return nil
}
