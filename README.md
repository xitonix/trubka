![GitHub release](https://img.shields.io/github/release/xitonix/trubka)
[![Build Status](https://travis-ci.org/xitonix/trubka.svg?branch=master)](https://travis-ci.org/xitonix/trubka)
[![Go Report Card](https://goreportcard.com/badge/github.com/xitonix/trubka)](https://goreportcard.com/report/github.com/xitonix/trubka)

<img src="_media/logo-small.jpg" alt="logo-small" align="left" width="300"/>

Trubka** is a Kafka CLI tool built in [Go](https://go.dev/) which gives you everything you need to 

* Manage, query and troubleshoot your Kafka clusters.
* Consume [protocol buffer](https://developers.google.com/protocol-buffers/) and plain text messages from Kafka.
* Publish protocol buffer and plain text messages to Kafka.


## Documentation

- [Installation](https://github.com/xitonix/trubka/wiki)
- [Cluster Administration](https://github.com/xitonix/trubka/wiki/Cluster-Administration)
- [Consuming from Kafka](https://github.com/xitonix/trubka/wiki/Consume-from-Kafka)
- [Publishing to Kafka](https://github.com/xitonix/trubka/wiki/Publish-to-Kafka)



## Acknoledgments

Special thanks to **Joshua Humphries** for building the fascinating [protoreflect](https://github.com/jhump/protoreflect) package. 

I would also like to mention some of the amazing libraries and packages I used for building Trubka:

- [sarama](https://github.com/Shopify/sarama) by the Shopify team

- [kingpin](https://github.com/alecthomas/kingpin) and [chroma](https://github.com/alecthomas/chroma) by Alec Thomas

- [tablewriter](https://github.com/olekukonko/tablewriter) by Oleku Konko

- [diskv](https://github.com/peterbourgon/diskv) by Peter Bourgon

- [gofakeit](https://github.com/brianvoe/gofakeit/) by Brian Voelker

- [go-humanize](https://github.com/dustin/go-humanize) by Dustin Sallings

- [confdir](https://github.com/kirsle/configdir) by Noah Petherbridge

- [go-homedir](https://github.com/mitchellh/go-homedir) by Mitchell Hashimoto

  

Last but definitely not least, thank you to all the great contributors to these repositories. Building Trubka would have been a pain without using your fantastic work. You are legends!



## Contribution and Feature Request

Trubka is built based on the problems I was trying to solve in my day to day work and I tried to build it as flexible as I could so that the community hopefully finds it useful too. That said, the tool by no means is compelete or issue free. I will openly accept PRs and feature requests to make sure we will have a better Trubka over time. 

Please feel free to play around with Trubka, log issues, open PRs and share your thoughts with the community and myself.

Thank you!





