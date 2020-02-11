package produce

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v4"
	"github.com/jhump/protoreflect/dynamic"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/protobuf"
)

type proto struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	proto        string
	count        uint32
	protoRoot    string
	random       bool
	protoMessage *dynamic.Message
	textEx       *regexp.Regexp
	emailEx      *regexp.Regexp
	intEx        *regexp.Regexp
	floatEx      *regexp.Regexp
	hashEx       *regexp.Regexp
	bytesEx      *regexp.Regexp
	intRangeEx   *regexp.Regexp
	floatRangeEx *regexp.Regexp
	rangeEx      *regexp.Regexp
}

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
		textEx:       regexp.MustCompile(`\?+`),
		emailEx:      regexp.MustCompile(`\s*\[Email]\s*`),
		intRangeEx:   regexp.MustCompile(`"(?i)\s*N\[\s*-?\s*\d+\s*:\s*-?\s*\d+\s*]\s*"`),
		// Integer range for none-numeric fields
		rangeEx:      regexp.MustCompile(`(?i)R\[\s*-?\s*\d+\s*:\s*-?\s*\d+\s*]`),
		floatRangeEx: regexp.MustCompile(`"(?i)\s*F\[\s*-?\s*[\d\.]+\s*:\s*-?\s*[\d\.]+\s*(:\s*[\d\.]+\s*)?\s*]"`),
		intEx:        regexp.MustCompile(`"(?i)\s*N\[.*]\s*"`),
		floatEx:      regexp.MustCompile(`"(?i)\s*F\[.*]\s*"`),
		bytesEx:      regexp.MustCompile(`(?i)\s*B64\[.*]\s*`),
		hashEx:       regexp.MustCompile(`#+`),
	}
	c := parent.Command("proto", "Publishes protobuf messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("proto", "The proto to publish to.").Required().StringVar(&cmd.proto)
	c.Arg("content", "The JSON representation of the message. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("count", "The number of messages to publish.").
		Default("1").
		Short('c').
		Uint32Var(&cmd.count)
	c.Flag("generate-random-data", "Replaces the random generator place holder functions with the random value.").
		Short('g').
		BoolVar(&cmd.random)
}

func (c *proto) run(_ *kingpin.ParseContext) error {
	value, err := getValue(c.message)
	if err != nil {
		return err
	}
	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	err = loader.Load(c.proto)
	if err != nil {
		return err
	}

	message, err := loader.Get(c.proto)
	if err != nil {
		return err
	}

	c.protoMessage = message

	return produce(c.kafkaParams, c.globalParams, c.topic, c.key, value, c.serializeProto, c.count)
}

func (c *proto) serializeProto(value string) ([]byte, error) {
	if c.random {
		v, err := c.replaceRandomGenerator(value)
		if err != nil {
			return nil, err
		}
		value = v
	}

	fmt.Println(value)
	err := c.protoMessage.UnmarshalJSON([]byte(value))
	if err != nil {
		return nil, err
	}

	return c.protoMessage.Marshal()
}

func (c *proto) replaceRandomGenerator(value string) (string, error) {
	gofakeit.Seed(time.Now().UnixNano())
	var err error
	value = c.replaceTextGenerators(value)
	// Ranges need to be processed before normal numbers
	value, err = c.replaceIntRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value, err = c.replaceFloatRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value, err = c.replaceNoneNumericRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value = c.replaceIntNumberGenerators(value)
	value = c.replaceFloatNumberGenerators(value)
	value = c.replaceAllNonNumericHashes(value)
	value = c.replaceBytesGenerators(value)
	return value, nil
}

func (c *proto) replaceTextGenerators(value string) string {
	value = c.textEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Lexify(match)
	})
	value = c.emailEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Email()
	})
	return value
}

func (c *proto) replaceBytesGenerators(value string) string {
	value = c.bytesEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "B64[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "]"))
		return base64.StdEncoding.EncodeToString([]byte(match))
	})
	return value
}

func (c *proto) replaceNoneNumericRangeGenerators(value string) (result string, err error) {

	result = c.rangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, "R[] ")
		parts := strings.Split(match, ":")
		if len(parts) != 2 {
			err = errors.New("the number range must be in m:n format")
		}
		from, e := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if e != nil {
			err = fmt.Errorf("the lower range value must be a valid integer: %w", e)
			return ""
		}
		to, e := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			err = fmt.Errorf("the upper range value must be a valid integer: %w", e)
			return ""
		}

		if from > to {
			err = fmt.Errorf("the upper range value (%d) must be greater than the lower range value (%d)", to, from)
			return ""
		}

		return strconv.FormatInt(int64(gofakeit.Number(int(from), int(to))), 10)
	})

	return
}

func (c *proto) replaceIntRangeGenerators(value string) (result string, err error) {

	result = c.intRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "N[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "\"]"))
		parts := strings.Split(match, ":")
		if len(parts) != 2 {
			err = errors.New("the range must be in m:n format")
		}
		from, e := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if e != nil {
			err = fmt.Errorf("the lower range value must be a valid integer: %w", e)
			return ""
		}
		to, e := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			err = fmt.Errorf("the upper range value must be a valid integer: %w", e)
			return ""
		}

		if from > to {
			err = fmt.Errorf("the upper range value (%d) must be greater than the lower range value (%d)", to, from)
			return ""
		}

		return strconv.FormatInt(int64(gofakeit.Number(int(from), int(to))), 10)
	})

	return
}

func (c *proto) replaceFloatRangeGenerators(value string) (result string, err error) {
	// Float range: F[from:to:<optional decimal places>]
	result = c.floatRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "F[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "\"]"))
		parts := strings.Split(match, ":")
		if len(parts) < 2 {
			err = errors.New("the range must be in m:n format")
		}
		from, e := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		if e != nil {
			err = fmt.Errorf("the lower range value must be a valid floating point number: %w", e)
			return ""
		}
		to, e := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if e != nil {
			err = fmt.Errorf("the upper range value must be a valid floating point number: %w", e)
			return ""
		}

		if from > to {
			err = fmt.Errorf("the upper range value (%f) must be greater than the lower range value (%f)", to, from)
			return ""
		}

		format := "%f"
		if len(parts) >= 3 {
			decimal, e := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 32)
			if e != nil {
				err = fmt.Errorf("the number of decimal places must be a valid positive integer: %w", e)
				return ""
			}
			format = fmt.Sprintf("%%.%df", decimal)
		}

		return fmt.Sprintf(format, gofakeit.Float64Range(from, to))
	})

	return
}

func (c *proto) replaceIntNumberGenerators(value string) string {
	return c.intEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "N[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "\"]"))
		return gofakeit.Numerify(match)
	})
}

func (c *proto) replaceFloatNumberGenerators(value string) string {
	return c.floatEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "F[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "\"]"))
		// The first zero will be replaced in gofakeit.Numerify!
		if len(match) > 0 && match[0] == '0' {
			match = strings.Replace(match, "0", "*^*", 1)
		}
		return strings.Replace(gofakeit.Numerify(match), "*^*", "0", 1)
	})
}

func (c *proto) replaceAllNonNumericHashes(value string) string {
	return c.hashEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Numerify(match)
	})
}
