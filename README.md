![GitHub release](https://img.shields.io/github/release/xitonix/trubka)
[![Build Status](https://travis-ci.org/xitonix/trubka.svg?branch=master)](https://travis-ci.org/xitonix/trubka)
[![Go Report Card](https://goreportcard.com/badge/github.com/xitonix/trubka)](https://goreportcard.com/report/github.com/xitonix/trubka)

**Trubka** Is a CLI tool to consume [protocol buffer](https://developers.google.com/protocol-buffers/) messages from Kafka. The tool uses Joshua's brilliant [protoreflect](https://github.com/jhump/protoreflect) package under the hood to deserialise the protobuf bytes without the need to compile the messages into Go types.



## Installation

### macOS

```bash
brew tap xitonix/trubka
brew install trubka
```

### Build from source

Clone the repo locally and build trubka from source.  You can also use `Make` to compile the code.

### Pre built binaries

Download the pre-built binaries for the platform of your choice from the [releases](https://github.com/xitonix/trubka/releases) page.

## Usage

```bash
usage: trubka [<flags>] <command> [<args> ...]

A tool to consume protocol buffer events from Kafka.

Flags:
      --help         Show context-sensitive help (also try --help-long and --help-man).
  -v, --verbose ...  The verbosity level of Trubka.

Commands:
  help [<command>...]
    Show help.

  version
    Prints the current version of Trubka.

  consume proto --proto-root=PROTO-ROOT [<flags>] [<topic>] [<proto>]
    Starts consuming protobuf encoded events from the given Kafka topic.

  consume plain [<flags>] [<topic>]
    Starts consuming plain text or json events from the given Kafka topic.

  broker list [<flags>]
    Lists the brokers in the Kafka cluster.

  topic list [<flags>]
    Loads the existing topics from the server.

  topic delete [<flags>]
    Deletes a topic.

  group list [<flags>]
    Lists the consumer groups.

  group delete [<flags>]
    Deletes an empty consumer group.

  group topics [<flags>] <group>
    Lists the topics a consumer group is subscribed to.

  local list [<flags>]
    Lists the local offsets for different environments.

  local delete [<flags>] <environment>
    Deletes the local offsets from the given environment.
```

## Consumer

Trubka can be used as a general purpose Kafka consumer. You can use it to consume from any protocol buffer or plain text (/json) topics.

###### Consuming Protobuf
```bash
trubka consume proto TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092
```

###### Consuming Plain Text (/Json)
```bash
trubka consume plain TopicA --brokers localhost:9092
```



### Interactive mode

Trubka can also be executed in interactive mode using the `-i` flag. Interactive mode walks you though the steps of picking the topic and the proto message type (if applicable) from a list of existing topics, fetched from the server, and a list of protocol buffer messages, living in the  `--proto-root` directory (if applicable). If you have too many topics on the server, the list can be narrowed down using `--topic-filter` flag. 

For proto consumer, the message type could also be filtered using `—proto-filter` flag.

###### Proto Consumer
```bash
trubka consume proto --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic-filter Notifications --proto-filter EmailSent -i
```

###### Plain Text Consumer
```bash
trubka consume plain --brokers localhost:9092 -i --topic-filter Notifications
```
**Note**

`--topic-filter` and `--proto-filter` flags are regular expressions.

### Searching Messages

You can optionally define a regular expression using the `-q` flag to filter the messages consumed from Kafka. It's simply a string match on the string representation of the de-serialised message content.

# SASL Authentication
Trubka supports the following SASL authentication mechanisms:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512

###### Proto Consumer

```bash
trubka consume proto TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092 \
--sasl-mechanism scram-sha-512 \
--sasl-username username --sasl-password password
```

###### Plain Text Consumer

```bash
trubka consume plain TopicA --brokers localhost:9092 \
--sasl-mechanism scram-sha-512 \
--sasl-username username --sasl-password password
```

# TLS

Trubka also supports TLS:

###### Proto Consumer

```bash
trubka consume proto TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--tls --ca-cert ~/certs/kafka.pem
```

###### Plain Text Consumer

```bash
trubka consume plain TopicA --brokers localhost:9092 \ 
--tls --ca-cert ~/certs/kafka.pem
```

To enable mutual authentication, you need to provide `--client-key` and `--client-cert` files as well.

# Environment Variables

It is possible to ask trubka to read the cli flags from the system environment variables. The flags must be in `TRUBKA_FLAG_NAME` format. For example the value of `--proto-root` parameter can be read from `TRUBKA_PROTO_ROOT` if it's provided.

Using environment variables is very useful in a multi-environment workspace.

**NOTE**

Providing the same cli flag when running trubka will override its environment variable counterpart. That means  `--proto-root=/tmp/protos` will override `TRUBKA_PROTO_ROOT=/dev/contracts` and trubka will run with `/tmp/protos` as proto root.

# Enable Auto Completion

In order to enable auto completion in bash or zsh, you can add the following line to your shell's startup script:

ZSH
```shell script
eval "$(trubka --completion-script-zsh)"
```

BASH
```shell script
eval "$(trubka --completion-script-bash)"
```