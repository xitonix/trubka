package kafka

import (
	"fmt"
	"sort"

	"github.com/xitonix/trubka/internal"
)

type API struct {
	Name       string
	Key        int16
	MinVersion int16
	MaxVersion int16
}

func newAPI(name string, key, minVer, maxVer int16) *API {
	if internal.IsEmpty(name) {
		name = "UNKNOWN"
	}
	return &API{
		Name:       name,
		Key:        key,
		MinVersion: minVer,
		MaxVersion: maxVer,
	}
}

func (a *API) String() string {
	return fmt.Sprintf("ver. %d ≤ %d%s ≤ ver. %d", a.MinVersion, a.Key, a.Name, a.MaxVersion)
}

type APIByCode []*API

func (a APIByCode) Len() int {
	return len(a)
}

func (a APIByCode) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a APIByCode) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

type BrokerMeta struct {
	ConsumerGroups []string
	Logs           []*LogFile
	APIs           []*API
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
