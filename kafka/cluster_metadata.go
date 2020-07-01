package kafka

type ClusterMetadata struct {
	Brokers       []*Broker      `json:"brokers,omitempty"`
	ConfigEntries []*ConfigEntry `json:"configurations,omitempty"`
}
