[![Build Status](https://travis-ci.org/xitonix/trubka.svg?branch=master)](https://travis-ci.org/xitonix/trubka)

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

```shell
trubka --proto-root /protocol_buffers_dir --kafka-endpoints localhost:9092 \
--topic-map "TopicA:MessageA, TopicB:MessageB"
```



### Interactive mode

Trubka can also be executed in interactive mode using the `-i` flag. Interactive mode walks you though the steps of picking topic(s) and proto message type(s) from provided lists of exising topics, fetched from the server, and a list of protocol buffer messages, living in the  `--proto-root` directory. If you have too many topics on the server, the list can be narrowed down using `--topic-filter` flag. The message type list could also be filtered using `—type-filter` flag.

```shell
trubka --proto-root /protocol_buffers_dir --kafka-endpoints localhost:9092 \
-i --topic-filter Notifications --type-filter EmailSent
```

##### Note

`topic-filter` and `type-filter` flags are regular expressions.