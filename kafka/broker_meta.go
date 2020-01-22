package kafka

import "sort"

type BrokerMeta struct {
	ConsumerGroups []string
	Logs           []*LogFile
}

type aggregatedTopicSize map[string]*LogSize

type LogFile struct {
	Path       string
	aggregated aggregatedTopicSize
}

func newLogFile(path string) *LogFile {
	return &LogFile{
		Path:       path,
		aggregated: make(aggregatedTopicSize),
	}
}

func (l *LogFile) set(topic string, size int64, isTemp bool) {
	if _, ok := l.aggregated[topic]; !ok {
		l.aggregated[topic] = &LogSize{
			Topic: topic,
		}
	}
	if isTemp {
		l.aggregated[topic].Temporary += uint64(size)
	} else {
		l.aggregated[topic].Permanent += uint64(size)
	}
}

func (l *LogFile) SortByPermanentSize() []*LogSize {
	result := l.toSlice()
	sort.Sort(logsByPermanentSize(result))
	return result
}

func (l *LogFile) toSlice() []*LogSize {
	result := make([]*LogSize, len(l.aggregated))
	var i int
	for _, l := range l.aggregated {
		result[i] = l
		i++
	}
	return result
}

type logsByPermanentSize []*LogSize

func (l logsByPermanentSize) Len() int {
	return len(l)
}

func (l logsByPermanentSize) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l logsByPermanentSize) Less(i, j int) bool {
	return l[i].Permanent > l[j].Permanent
}

type LogSize struct {
	Topic     string
	Permanent uint64
	Temporary uint64
}
