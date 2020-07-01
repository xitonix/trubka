package kafka

import (
	"fmt"
	"sort"

	"github.com/xitonix/trubka/internal"
)

type API struct {
	Name       string `json:"name"`
	Key        int16  `json:"key"`
	MinVersion int16  `json:"min_version"`
	MaxVersion int16  `json:"max_version"`
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
	return fmt.Sprintf("v%d ≤ [%2d] %s ≤ v%d", a.MinVersion, a.Key, a.Name, a.MaxVersion)
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
	IsController   bool
	ConsumerGroups []string
	Logs           []*LogFile
	APIs           []*API
}

func (b *BrokerMeta) ToJson(withLogs, withAPIs, includeZeros bool) interface{} {
	if b == nil {
		return nil
	}
	type log struct {
		Path    string     `json:"path"`
		Entries []*LogSize `json:"entries"`
	}
	output := struct {
		IsController   bool     `json:"controller"`
		ConsumerGroups []string `json:"consumer_groups"`
		Logs           []*log   `json:"logs,omitempty"`
		APIs           []*API   `json:"api,omitempty"`
	}{
		IsController:   b.IsController,
		ConsumerGroups: b.ConsumerGroups,
	}

	if withLogs {
		for _, logs := range b.Logs {
			sorted := logs.SortByPermanentSize()
			log := &log{
				Path:    logs.Path,
				Entries: []*LogSize{},
			}
			for _, entry := range sorted {
				if !includeZeros && entry.Permanent == 0 {
					continue
				}
				log.Entries = append(log.Entries, entry)
			}
			output.Logs = append(output.Logs, log)
		}
	}

	if withAPIs {
		sort.Sort(APIByCode(b.APIs))
		output.APIs = b.APIs
	}

	return output
}

type aggregatedTopicSize map[string]*LogSize

type LogFile struct {
	Path    string              `json:"path"`
	Entries aggregatedTopicSize `json:"entries"`
}

func newLogFile(path string) *LogFile {
	return &LogFile{
		Path:    path,
		Entries: make(aggregatedTopicSize),
	}
}

func (l *LogFile) set(topic string, size int64, isTemp bool) {
	if _, ok := l.Entries[topic]; !ok {
		l.Entries[topic] = &LogSize{
			Topic: topic,
		}
	}
	if isTemp {
		l.Entries[topic].Temporary += uint64(size)
	} else {
		l.Entries[topic].Permanent += uint64(size)
	}
}

func (l *LogFile) SortByPermanentSize() []*LogSize {
	result := l.toSlice()
	sort.Sort(logsByPermanentSize(result))
	return result
}

func (l *LogFile) toSlice() []*LogSize {
	result := make([]*LogSize, len(l.Entries))
	var i int
	for _, l := range l.Entries {
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
	Topic     string `json:"topic"`
	Permanent uint64 `json:"permanent"`
	Temporary uint64 `json:"temporary"`
}
