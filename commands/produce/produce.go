package produce

import (
	"io/ioutil"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("produce", "A command to publish messages to kafka.")
	addPlainSubCommand(parent, global, kafkaParams)
	addProtoSubCommand(parent, global, kafkaParams)
	addSchemaSubCommand(parent, global)
}

func initialiseProducer(kafkaParams *commands.KafkaParameters, verbosity internal.VerbosityLevel) (*kafka.Producer, error) {

	saramaLogWriter := ioutil.Discard
	if verbosity >= internal.Chatty {
		saramaLogWriter = os.Stdout
	}

	brokers := commands.GetBrokers(kafkaParams.Brokers)
	producer, err := kafka.NewProducer(
		brokers,
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword))

	if err != nil {
		return nil, err
	}
	return producer, nil
}
