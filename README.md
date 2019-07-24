[![Build Status](https://travis-ci.org/xitonix/trubka.svg?branch=master)](https://travis-ci.org/xitonix/trubka)

**Trubka** Is a CLI tool to consume [protocol buffer](https://developers.google.com/protocol-buffers/) messages from Kafka. The tool uses Joshua's brilliant [protoreflect](https://github.com/jhump/protoreflect) package under the hood to deserialise the protobuf bytes without the need to compile the messages into Go types.



## Installation

Clone the repo locally and build trubka from source

**OR**

Download the pre-built binaries for the platform of your choice from the [releases](https://github.com/xitonix/trubka/releases) page.



## Usage

```shell
./trubka --proto-root /protocol_buffers_dir --kafka-endpoints localhost:9092 --topic-map "TopicA:MessageA, TopicB:MessageB"
```