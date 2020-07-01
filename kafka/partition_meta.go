package kafka

type PartitionMeta struct {
	Id              int32     `json:"id"`
	Offset          int64     `json:"offset"`
	Leader          *Broker   `json:"leader"`
	Replicas        []*Broker `json:"replicas"`
	ISRs            []*Broker `json:"in_sync_replicas"`
	OfflineReplicas []*Broker `json:"offline_replicas"`
}

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
