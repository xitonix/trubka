package kafka

// PartitionMeta represents partition metadata.
type PartitionMeta struct {
	// Id partition id.
	Id int32 `json:"id"`
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

// PartitionMetaById sorts partition metadata by partition Id.
type PartitionMetaById []*PartitionMeta

func (b PartitionMetaById) Len() int {
	return len(b)
}

func (b PartitionMetaById) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b PartitionMetaById) Less(i, j int) bool {
	return b[i].Id < b[j].Id
}
