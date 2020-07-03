package kafka

// ClusterMetadata Kafka cluster metadata.
type ClusterMetadata struct {
	// Brokers the list of the brokers within the cluster.
	Brokers []*Broker `json:"brokers,omitempty"`
	// ConfigEntries cluster configuration entries.
	ConfigEntries []*ConfigEntry `json:"configurations,omitempty"`
}
