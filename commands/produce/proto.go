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
	"github.com/xitonix/trubka/internal"
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
}

const (
	floatEx            = `[-+]?[0-9]*\.?[0-9]+`
	intEx              = `[0-9]+`
	signedIntEx        = `[-+]?[0-9]+`
	intPlaceHolderEx   = "([0-9]|#)*"
	floatPlaceHolderEx = `[-+]?([0-9]|#)*\.?([0-9]|#)+`
)

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {

	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
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
	// Ranges need to be processed before normal numbers
	value, err = c.replaceIntRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value, err = c.replaceFloatRangeGenerators(value)
	if err != nil {
		return "", err
	}
	value = c.replaceIntNumberGenerators(value)
	value = c.replaceFloatNumberGenerators(value)
	value = c.replaceExtraGenerators(value)
	value = c.replaceB64Generators(value)
	return value, nil
}

func (c *proto) replaceExtraGenerators(value string) string {
	matchers := []struct {
		ex       *regexp.Regexp
		replacer func(match string) string
	}{
		{
			ex: regexp.MustCompile(`(Email|EmailAddress)\(\)`),
			replacer: func(match string) string {
				return gofakeit.Email()
			},
		},
		{
			ex: regexp.MustCompile(`IP\(v4\)`),
			replacer: func(match string) string {
				return gofakeit.IPv4Address()
			},
		},
		{
			ex: regexp.MustCompile(`IP\(v6\)`),
			replacer: func(match string) string {
				return gofakeit.IPv6Address()
			},
		},
		{
			ex: regexp.MustCompile(`FirstName\(\)`),
			replacer: func(match string) string {
				return gofakeit.FirstName()
			},
		},
		{
			ex: regexp.MustCompile(`LastName\(\)`),
			replacer: func(match string) string {
				return gofakeit.LastName()
			},
		},
		{
			ex: regexp.MustCompile(`State\(\)`),
			replacer: func(match string) string {
				return gofakeit.State()
			},
		},
		{
			ex: regexp.MustCompile(`S\([\w\?]+\)`),
			replacer: func(match string) string {
				return gofakeit.Lexify(match[2 : len(match)-1])
			},
		},
		{
			ex: regexp.MustCompile(`(?i)Pick\(.*\)`),
			replacer: func(match string) string {
				items := match[strings.Index(match, "[")+1 : strings.Index(match, "]")]
				if internal.IsEmpty(items) {
					return match
				}
				list := strings.FieldsFunc(items, func(r rune) bool {
					return r == ',' || r == ' '
				})
				return gofakeit.RandString(list)
			},
		},
	}

	for _, matcher := range matchers {
		value = matcher.ex.ReplaceAllStringFunc(value, matcher.replacer)
	}

	return value
}

func (c *proto) replaceB64Generators(value string) string {
	base64Ex := regexp.MustCompile(`B64\(.*\)`)
	value = base64Ex.ReplaceAllStringFunc(value, func(match string) string {
		m := gofakeit.Lexify(match[2 : len(match)-1])
		return base64.StdEncoding.EncodeToString([]byte(m))
	})
	return value
}

func (c *proto) replaceIntRangeGenerators(value string) (result string, err error) {
	intRangeEx := regexp.MustCompile(fmt.Sprintf(`"\s*N\(%s:%[1]s\)\s*"|NS\(%[1]s:%[1]s\)`, signedIntEx))
	result = intRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, getCutSet(match, "N"))
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
	floatRangeEx := regexp.MustCompile(fmt.Sprintf(`"\s*F\(%s:%[1]s(:%s)?\)\s*"|FS\(%[1]s:%[1]s(:%[2]s)?\)`, floatEx, intEx))
	result = floatRangeEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, getCutSet(match, "F"))
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
	intEx := regexp.MustCompile(fmt.Sprintf(`"\s*N\(%s\)\s*"|NS\(%[1]s\)`, intPlaceHolderEx))
	return intEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, getCutSet(match, "N"))
		return gofakeit.Numerify(match)
	})
}

func (c *proto) replaceFloatNumberGenerators(value string) string {
	floatEx := regexp.MustCompile(fmt.Sprintf(`"\s*F\(%s\)\s*"|FS\(%[1]s\)`, floatPlaceHolderEx))
	return floatEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Trim(match, getCutSet(match, "F"))
		// The first zero will be replaced in gofakeit.Numerify!
		if len(match) > 0 && match[0] == '0' {
			match = strings.Replace(match, "0", "*^*", 1)
		}
		return strings.Replace(gofakeit.Numerify(match), "*^*", "0", 1)
	})
}

func getCutSet(match, fn string) string {
	prefix := fn + "S"
	if strings.Contains(match, prefix) {
		return prefix + "()"
	}
	return fn + "()\" "
}
