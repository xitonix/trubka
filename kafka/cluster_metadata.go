package kafka

type ClusterMetadata struct {
	ConfigEntries []*ConfigEntry
	Brokers       []*Broker
}
