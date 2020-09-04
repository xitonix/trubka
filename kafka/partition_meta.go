package kafka

// PartitionMeta represents partition metadata.
type PartitionMeta struct {
	// ID partition id.
	ID int32 `json:"id"`
	// Offset partition offset.
	Offset int64 `json:"offset"`
	// Leader leader node.
	Leader *Broker `json:"leader"`
	// Replicas replication nodes.
	Replicas []*Broker `json:"replicas"`
	// ISRs in-sync replicas.
	ISRs []*Broker `json:"in_sync_replicas"`
	// OfflineReplicas offline replicas.
	OfflineReplicas []*Broker `json:"offline_replicas"`
}

// PartitionMetaByID sorts partition metadata by partition ID.
type PartitionMetaByID []*PartitionMeta

func (b PartitionMetaByID) Len() int {
	return len(b)
}

func (b PartitionMetaByID) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b PartitionMetaByID) Less(i, j int) bool {
	return b[i].ID < b[j].ID
}
