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
trubka --proto-root /protocol_buffers_dir --brokers localhost:9092 \
--topic TopicA --proto MessageA
```



### Interactive mode

Trubka can also be executed in interactive mode using the `-i` flag. Interactive mode walks you though the steps of picking topic(s) and proto message type(s) from provided lists of exising topics, fetched from the server, and a list of protocol buffer messages, living in the  `--proto-root` directory. If you have too many topics on the server, the list can be narrowed down using `--topic-filter` flag. The message type list could also be filtered using `—type-filter` flag.

```bash
trubka --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic-filter Notifications --type-filter EmailSent -i
```

##### Note

`topic-filter` and `type-filter` flags are regular expressions.

### Searching Messages

You can optionally define a regular expression using the `-q` flag to filter the messages consumed from Kafka. It's simply a string match on the string representation of the deserialised message content.

# SASL Authentication
Trubka supports the following SASL authentication mechanisms:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512

```bash
trubka --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic TopicA --proto MessageA --sasl-mechanism scram-sha-512 \
--sasl-username username --sasl-password password
```

# TLS

Trubka also supports verified (and unverified) TLS communication to the given Kafka cluster:

```bash
trubka --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic TopicA --proto MessageA --tls ~/certs/kafka.pem
```

**ATTENTION**

In unverified mode (when no CA file has been provided), Trubka accepts any certificate presented by the cluster and any host name in that certificate. Please be mindful that TLS will be open to man-in-the-middle attacks in this mode and **this should be used for testing purposes only**.