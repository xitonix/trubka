![GitHub release](https://img.shields.io/github/release/xitonix/trubka)
[![Build Status](https://travis-ci.org/xitonix/trubka.svg?branch=master)](https://travis-ci.org/xitonix/trubka)
[![Go Report Card](https://goreportcard.com/badge/github.com/xitonix/trubka)](https://goreportcard.com/report/github.com/xitonix/trubka)

**Trubka** Is a CLI tool to consume [protocol buffer](https://developers.google.com/protocol-buffers/) messages from Kafka. The tool uses Joshua's brilliant [protoreflect](https://github.com/jhump/protoreflect) package under the hood to deserialise the protobuf bytes without the need to compile the messages into Go types.



## Installation

### macOS

```bash
$> brew tap xitonix/trubka
$> brew install trubka
```

### Build from source

Clone the repo locally and build trubka from source.  You can also use `Make` to compile the code.

### Pre built binaries

Download the pre-built binaries for the platform of your choice from the [releases](https://github.com/xitonix/trubka/releases) page.

## Usage

```bash
usage: trubka [<flags>] <command> [<args> ...]

A CLI tool for Kafka.

Flags:
      --help                     Show context-sensitive help (also try --help-long and --help-man).
      --color                    Enables colors in the standard output. To disable, use --no-color (Disabled by default on Windows).
  -v, --verbose ...              The verbosity level of Trubka.
  -b, --brokers=BROKERS          The comma separated list of Kafka brokers in server:port format.
      --kafka-version="2.4.0"    Kafka cluster version.
      --sasl-mechanism=none      SASL authentication mechanism.
  -U, --sasl-username=SASL-USERNAME
                                 SASL authentication username. Will be ignored if --sasl-mechanism is set to none.
  -P, --sasl-password=SASL-PASSWORD
                                 SASL authentication password. Will be ignored if --sasl-mechanism is set to none.
      --tls                      Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.
      --ca-cert=CA-CERT          Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.
      --client-cert=CLIENT-CERT  Client certification file to enable mutual TLS authentication. Client key must also be provided.
      --client-key=CLIENT-KEY    Client private key file to enable mutual TLS authentication. Client certificate must also be provided.

Commands:
  help [<command>...]
    Show help.

  version
    Prints the current version of Trubka.

  list topics [<flags>]
    Loads the existing topics from the server.

  list groups [<flags>]
    Loads the consumer groups from the server.

  list group-offsets [<flags>] <group>
    Lists a consumer group's offsets for all the topics within the group.

  list local-offsets [<flags>] <topic> <environment>
    Lists the locally stored offsets of the given topic and environment.

  list local-topics [<flags>]
    Lists the locally stored topics and the environments.

  describe group [<flags>] <group>
    Describes a consumer group.

  describe broker [<flags>] <broker>
    Describes a Kafka broker.

  describe topic [<flags>] <topic>
    Describes a Kafka topic.

  describe cluster [<flags>]
    Describes the Kafka cluster.

  delete topic [<flags>] <topic>
    Deletes a topic.

  delete group [<flags>] <group>
    Deletes a consumer group.

  delete local-offsets <environment> [<topic>]
    Deletes the local offsets from the given environment.

  consume proto --proto-root=PROTO-ROOT [<flags>] [<topic>] [<contract>]
    Starts consuming protobuf encoded events from the given Kafka topic.

  consume plain [<flags>] [<topic>]
    Starts consuming plain text or json events from the given Kafka topic.

  create topic --number-of-partitions=NUMBER-OF-PARTITIONS --replication-factor=REPLICATION-FACTOR [<flags>] <topic>
    Creates a new topic.

  create partitions --number-of-partitions=NUMBER-OF-PARTITIONS <topic>
    Increases the number of partitions of the given topic. If the topic has a key, the partition logic or ordering of the messages will be affected.

  produce plain [<flags>] <topic> [<content>]
    Publishes plain text messages to Kafka. The content can be arbitrary text, json, base64 or hex encoded strings.

  produce proto --proto-root=PROTO-ROOT [<flags>] <topic> <proto> [<content>]
    Publishes protobuf messages to Kafka.

  produce schema --proto-root=PROTO-ROOT [<flags>] <proto>
    Produces the JSON representation of the given proto message. The produced schema can be used to publish to Kafka.
```

## Consume from Kafka

Trubka can be used as a general purpose Kafka consumer. You can use it to consume from any protocol buffer or plain text topics.

###### Consuming Protobuf
```bash
$> trubka consume proto TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092
```

###### Consuming Plain Text (Arbitrary Text/Json or Base64/Hex encoded strings)
```bash
$> trubka consume plain TopicA --brokers localhost:9092
```

### Interactive mode

Trubka can also be executed in interactive mode using the `-i` flag. Interactive mode walks you though the steps of picking the topic and the proto message type (if applicable) from a list of existing topics, fetched from the server, and a list of protocol buffer messages, living in the  `--proto-root` directory (if applicable). If you have too many topics on the server, the list can be narrowed down using `--topic-filter` flag. 

For proto consumer, the message type could also be filtered using `—proto-filter` flag.

###### Proto Consumer
```bash
$> trubka consume proto --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--topic-filter Notifications --proto-filter EmailSent -i
```

###### Plain Text Consumer
```bash
$> trubka consume plain --brokers localhost:9092 -i --topic-filter Notifications
```
**Note**

`--topic-filter` and `--proto-filter` flags are regular expressions.

### Searching Messages

You can optionally define a regular expression using the `-q` flag to filter the messages consumed from Kafka. It's simply a string match on the string representation of the de-serialised message content.

## Publish to Kafka

You can use Trubka to publish proto or plain text messages to Kafka. 

### Plain Text Producer

You can use this command to push an arbitrary piece of plain text message into Kafka.

```bash
$ echo 'Random Data' | trubka produce plain TopicA --brokers localhost:9092
OR
$ trubka produce plain TopicA 'Random Data' --brokers localhost:9092
```



### Proto Producer

The protocol buffer bytes can be provided to the proto producer using one the following methods:
- Hex/Base64 encoded string of the proto bytes
- Json template

The first scenario is pretty straightforward. You have the protocol buffer bytes in Hex or Base64 format and all you need is to feed the bytes in:

```bash
$> echo 'Hex/Base64 encoded bytes' | trubka produce proto TopicA MessageA \
--proto-root /protocol_buffers_dir --brokers localhost:9092
```

OR

```bash
$> trubka produce proto ToicA MessageA 'Hex/Base64 encoded bytes' \
--proto-root /protocol_buffers_dir --brokers localhost:9092
```

But this method is only useful when you already have the protocol buffer bytes, which is not the case most of the time. To solve that problem, Trubka lets you provide the Json representation of your message and it automatically converts it to proto bytes, as long as the schemas are compatible. For example, if you have the following schema in your `--proto-root` directory:

```protobuf
syntax = "proto3";

package contracts;

option go_package = "git.ourdomain.com/contracts";
import "google/protobuf/timestamp.proto";

message EntityDefined {
  string entity_id = 1;
  google.protobuf.Timestamp defined_date_utc = 2;
  oneof authentication_oneof {
    string api_key = 3;
    string token = 4;
  }
}
```

You can use the following Json payload to publish a proto message to Kafka:

```json
{
  "entityId": "entity id",
  "definedDateUtc": "2020-02-17T23:36:13Z",
  "apiKey": "api key"
}
```

To make this even easier, Trubka comes with a useful command which can generate the Json schema, based on the protocol buffer contract:

```bash
$> trubka produce schema contracts.EntityDefined --proto-root /protocol_buffers_dir > EntityDefined.json
$> cat EntityDefined.json | trubka produce proto TopicA contracts.EntityDefined --proto-root /protocol_buffers_dir
```

Not happy yet? Trubka also comes with a super flexible templating language (based on the amazing [gofakeit](https://github.com/brianvoe/gofakeit) library) which lets you randomize the Json representation of each message. Run the `produce schema` command with `-g` and you will see the magic.

```bash
$> trubka produce schema contracts.EntityDefined --proto-root /protocol_buffers_dir -g > EntityDefined.json
```

##### EntityDefined.json
```json
{
    "api_key": "Str(?????)",
    "defined_date_utc": "Now('2006-01-02T15:04:05Z07:00','UTC')",
    "entity_id": "Str(?????)"
 }

// This will be translated into a random json payload before converting to proto bytes
{
    "api_key": "xhyed",
    "defined_date_utc": "2020-02-18T03:10:42Z",
    "entity_id": "mhaac"
}
```


Remember to provide `-g` to the `produce proto` command to instruct Trubka to replace the template functions with the actual value. 

So, to publish 100 random proto messages into your topic, all you need is to run the following command:


```bash
$> cat EntityDefined.json | trubka produce proto TopicA contracts.EntityDefined -g -c 100 --proto-root /protocol_buffers_dir
```

Here is the list of template functions supported by Trubka:

|          **Function(s)**           | **Description**                                              | Usage/Example                                                |
| :--------------------------------: | :----------------------------------------------------------- | :----------------------------------------------------------- |
|             Str(????)              | Generates a random string by replacing each `?` with a random letter. | Str(??): *gd*                                                |
|             Int(####)              | Generates a random integer by replacing each `#` with a random digit. | Int(##): *73*<br />Int(2#1): *281*                           |
|             IntS(####)             | Generates the string representation of an integer by replacing each `#` with a random digit. | IntS(##): *"73"*                                             |
|            Int(from,to)            | Generates an integer between `from` and `to`.                | Int(10,20): *14*                                             |
|           IntS(from,to)            | Generates the string representation of an integer between `from` and `to`. | IntS(10,20): *"14"*                                          |
|            Float(##.##)            | Generates a random floating point number by replacing each `#` with a random digit. | Float(#.##): *1.18*<br />Float(0.#2): *0.82*                 |
|           FloatS(##.##)            | Generates the string representation of a random floating point number by replacing each `#` with a random digit. | FloatS(#.#): *"1.2"*<br />FloatS(1.#2): *"1.72"*             |
|  Float(from,to,[decimal places])   | Generates a floating point number between `from` and `to` with the optional limit for decimal places. | Float(0.1,1.5): *0.75*                                       |
|  FloatS(from,to,[decimal places])  | Generates the string representation of an floating point number between `from` and `to`. | FloatS(0.1,1.5): *"0.75"*                                    |
|               Bool()               | true or false                                                |                                                              |
|              BoolS()               | "true" or "false"                                            |                                                              |
|               IP(v4)               | Generates a random IP v4 string.                             | *"*248.177.118.254*"*                                        |
|               IP(v6)               | Generates a random IP v6 string.                             | *"fde8:4372:2fcc:86e5:ffff:ffff:ffff:ffff"*                  |
|            MacAddress()            | Generates a random MAC address string.                       | *"e1:74:cb:01:77:91"*                                        |
| Timestamp(['layout'],['timezone']) | The layout must follow the Go's [time formatting](https://golang.org/pkg/time/#Time.Format) standard (default RFC3339) and the Timezone can be either a standard IANA value or a UTC offset in `UTC±hh:mm` format. **Both parameters must be enclosed by single quotes if provided**. | Timestamp('2006-01-02T15:04:05 MST')<br /><br />Timestamp('2006-01-02T15:04:05Z07:00','UTC')<br /><br />Timestamp('2006-01-02T15:04:05','UTC+10') |
|    Now(['layout'],['timezone'])    | Generates the current time. See the Timestamp explanation above for more details. | Now('2006-01-02T15:04:05')<br /><br />Now('15:04:05Z07:00','UTC')<br /><br />Now('2006-01-02T15:04:05','UTC-8') |
|              B64(...)              | Generates the base64 encoded value of its input.             | B64(Str(???))<br />B64(IP(v4))<br />B64(random data)         |
|    Email()<br />EmailAddress()     | Generates a random email address.                            | *"address@domain.com"*                                       |
|               Name()               | Generates a random full name.                                | *"John Smith"*                                               |
|            FirstName()             | Generates a random first name.                               | *"John"*                                                     |
|             LastName()             | Generates a random last name.                                | *"Smith"*                                                    |
|            NamePrefix()            | Generates a random title.                                    | *"Mr."*                                                      |
|            NameSuffix()            | Generates a random name suffix.                              | *"Jr."*                                                      |
|             Country()              | Generates a random country name.                             | *"Australia"*                                                |
|            CountryAbr()            | Generates a random country abbreviation.                     | *"FI"*                                                       |
|              State()               | Generates a random US state name.                            | *"California"*                                               |
|             StateAbr()             | Generates a random US state abbreviation.                    | *"California"*                                               |
|              Street()              | Generates a random street address.                           | *"364 East Rapidsborough"*                                   |
|            StreetName()            | Generates a random street name.                              | *"View"*                                                     |
|           StreetPrefix()           | Generates a random string prefix.                            | *"Lake"*                                                     |
|            StreetSuffix            | Generates a random string suffix.                            | *"land"*                                                     |
|               City()               | Generates a random city name.                                | *"Marcelside"*                                               |
|               UUID()               | Generates a random Universal Unique Identifier (UUID)        | *"5d093de6-c2e3-423d-87ad-c31f31d0e341"*                     |
|          Color()/Colour()          | Generates a random colour name.                              | *"MediumOrchid"*                                             |
|       HexColor()/HexColour()       | Generates the hex code of a random colour.                   | *"#a99fb4"*                                                  |
|             Currency()             | Generates a random currency name.                            | *"Australian Dollar"*                                        |
|           CurrencyAbr()            | Generates a random currency abbreviation.                    | *"USD"*                                                      |
|              Gender()              | *"male"* or *"female"*                                       |                                                              |
|               URL()                | Generates a random URL                                       | *"http://xitonix.io"*                                        |
|       ProgrammingLanguage()        | Generates a random programming language                      | *"Go"*                                                       |
|            DomainName()            | Generates a random HTTP domain name.                         | *"google.com"*                                               |
|           DomainSuffix()           | Generates a random HTTP domain suffix.                       | *"org"*                                                      |
|            UserAgent()             | Generates a random browser user agent string.                | *"Mozilla/5.0 (Windows NT 5.0) AppleWebKit/5362 (KHTML, like Gecko) Chrome/37.0.834.0 Mobile Safari/5362"* |
|             Username()             | Generates a random username.                                 | *"Alex1364"*                                                 |
|             TimeZone()             | Generates a random timezone name.                            | *"Kaliningrad Standard Time"*                                |
|           TimeZoneFull()           | Generates a random full timezone name.                       | *"(UTC+03:00) Kaliningrad, Minsk"*                           |
|           TimeZoneAbr()            | Generates a random timezone abbreviation.                    | *"KST"*                                                      |
|              Month()               | Generates a random month name.                               | *"January"*                                                  |
|             WeekDay()              | Generates a random weekday.                                  | *"Friday"*                                                   |
|            HTTPMethod()            | Generates a random HTTP verb.                                | *"GET"*, *"POST"*, etc                                       |
|           Pick(args...)            | Randomly chooses an item from the list. This function is useful to choose an item from numeric values. | Pick(1,100) may choose *1* or *100*                          |
|           PickS(args...)           | Randomly chooses the string representation of an item from the list. | Pick(1,Go,true) may choose *"true"*, *"Go"* or *"1"*         |
|             PetName()              | Generates a random pet name.                                 | *Ozzy Pawsborne*                                             |
|              Animal()              | Generates a random animal name.                              | *"elk"*                                                      |
|            FarmAnimal()            | Generates a random farm animal name.                         | *"Chicken"*                                                  |
|            AnimalType()            | Generates a random animal type.                              | *"amphibians"*                                               |
|               Cat()                | Generates a random cat name.                                 | *"Chausie"*                                                  |
|               Dog()                | Generates a random dog name.                                 | *"Norwich Terrier"*                                          |
|             BeerName()             | Generates a random beer name.                                | *"Duvel"*                                                    |
|            BeerStyle()             | Generates a random beer style.                               | *"European Amber Lager"*                                     |
|             BuzzWord()             | Generates a random buzz word.                                | *"Disintermediate"*                                          |
|             CarMaker()             | Generates a random car maker name.                           | *"Nissan"*                                                   |
|             CarModel()             | Generates a random car model.                                | *"Aveo"*                                                     |
|             Company()              | Generates a random company name.                             | *"Moen, Pagac and Wuckert"*                                  |
|          CompanySuffix()           | Generates a random company suffix.                           | *"Inc"*                                                      |
|          CreditCardCvv()           | Generates a random credit card Cvv code.                     | *"043"*                                                      |
|          CreditCardExp()           | Generates a random credit card expierety date.               | *"05/22"*                                                    |
|         CreditCardNumber()         | Generates a random credit card number.                       | *4136459948995369*                                           |
|        CreditCardNumberS()         | Generates a random credit card number string.                | *"4136459948995369"*                                         |
|       CreditCardNumberLuhn()       | Generates a random credit card number int that passes [luhn test](https://en.wikipedia.org/wiki/Luhn_algorithm). | *2720996615546177*                                           |
|      CreditCardNumberLuhnS()       | Generates a random credit card number string that passes [luhn test.](https://en.wikipedia.org/wiki/Luhn_algorithm) | *"2720996615546177"*                                         |
|          CreditCardType()          | Generates a random credit card type.                         | *"Visa"*                                                     |
|             MimeType()             | Generates a random mime type.                                | *"application/json"*                                         |
|             Language()             | Generates a random language name.                            | *"French"*                                                   |
|              Phone()               | Generates a random phone number.                             | *"6136459948"*                                               |
|          PhoneFormatted()          | Generates a random formatted phone number.                   | *"136-459-9489"*                                             |
|     Sentence(number of words)      | Generates a random sentence with the given number of words.  | Sentence(5): *"Quia quae repellat consequatur quidem."*      |

**Note**

The name of the template functions are case sensitive.

#### Combining Template functions

You can also combine the template functions together whenever it makes sense. Here are a few examples:

```json
{
  "user": {
    "id": "FirstName():LastName():IntS(10,50)"
    "machine": "MacAddress()-IP(v4)",
    "token": "B64(T-Str(????)-IntS(#####))"
  },
  "origin": "PickS(IP(v4), IP(v6))",
  "credit_card": "CreditCardNumberS() CreditCardCvv() CreditCardExp()"
}
```

# SASL Authentication

Trubka supports the following SASL authentication mechanisms:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512

###### Example


```bash
$> trubka consume plain TopicA --brokers localhost:9092 \
--sasl-mechanism scram-sha-512 \
--sasl-username username --sasl-password password
```

# TLS

Trubka also supports TLS:

###### Proto Consumer

```bash
$> trubka consume proto TopicA MessageA --proto-root /protocol_buffers_dir --brokers localhost:9092 \ 
--tls --ca-cert ~/certs/kafka.pem
```

###### Plain Text Consumer

```bash
$> trubka consume plain TopicA --brokers localhost:9092 \ 
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