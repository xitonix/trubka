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
trubka consume TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092
```



### Interactive mode

Trubka can also be executed in interactive mode using the `-i` flag. Interactive mode walks you though the steps of picking topic(s) and proto message type(s) from provided lists of exising topics, fetched from the server, and a list of protocol buffer messages, living in the  `--proto-root` directory. If you have too many topics on the server, the list can be narrowed down using `--topic-filter` flag. The message type list could also be filtered using `—type-filter` flag.

```bash
trubka consume --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic-filter Notifications --proto-filter EmailSent -i
```

##### Note

`topic-filter` and `proto-filter` flags are regular expressions.

### Searching Messages

You can optionally define a regular expression using the `-q` flag to filter the messages consumed from Kafka. It's simply a string match on the string representation of the deserialised message content.

# SASL Authentication
Trubka supports the following SASL authentication mechanisms:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512

```bash
trubka consume TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092 \
--sasl-mechanism scram-sha-512 \
--sasl-username username --sasl-password password
```

# TLS

Trubka also supports TLS:

```bash
trubka consume TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--tls --ca-cert ~/certs/kafka.pem
```
To enable mutual authentication, you need to provide `--client-key` and `--client-cert` files.

# Environment Variables

It is possible to ask trubka to read the cli flags from the system environment variables. The flags must be in `TRUBKA_FLAG_NAME` format. For example the value of `--proto-root` parameter can be read from `TRUBKA_PROTO_ROOT` if it's provided.

**NOTE**

Providing the same cli flag when running trubka will override its environment variable counterpart. That means  `--proto-root=/tmp/protos` will override `TRUBKA_PROTO_ROOT=/dev/contracts` and trubka will run with `/tmp/protos` as proto root. 