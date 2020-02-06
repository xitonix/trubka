package kafka

type PartitionMeta struct {
	Id              int32
	ISRs            []*Broker
	Replicas        []*Broker
	OfflineReplicas []*Broker
	Leader          *Broker
	Offset          int64
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
