package kafka

import (
	"fmt"
	"sort"

	"github.com/xitonix/trubka/internal"
)

// API represents a Kafka API.
type API struct {
	// Name method name.
	Name string `json:"name"`
	// Key the key of the API method.
	Key int16 `json:"key"`
	// MinVersion minimum version the broker supports.
	MinVersion int16 `json:"min_version"`
	// MaxVersion maximum version the broker supports.
	MaxVersion int16 `json:"max_version"`
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

// String returns the string representation of the API.
func (a *API) String() string {
	return fmt.Sprintf("v%d ≤ [%2d] %s ≤ v%d", a.MinVersion, a.Key, a.Name, a.MaxVersion)
}

// APIByCode sorts the API list by code.
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

// BrokerMeta holds a Kafka broker's metadata.
type BrokerMeta struct {
	// IsController is true if the broker is a controller node.
	IsController bool
	// ConsumerGroups a list of the consumer groups being managed by the broker.
	ConsumerGroups []string
	// Logs broker logs.
	Logs []*LogFile
	// API a list of the APIs supported by the broker.
	APIs []*API
}

// ToJson returns an object ready to be serialised into json string.
func (b *BrokerMeta) ToJson(withLogs, withAPIs, includeZeros bool) interface{} {
	if b == nil {
		return nil
	}
	type log struct {
		Path    string      `json:"path"`
		Entries []*LogEntry `json:"entries"`
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
				Entries: []*LogEntry{},
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

type aggregatedTopicSize map[string]*LogEntry

// LogFile represents a broker log file.
type LogFile struct {
	// Path the path on the server where the log is being stored.
	Path string `json:"path"`
	// Entries the log entries.
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
		l.Entries[topic] = &LogEntry{
			Topic: topic,
		}
	}
	if isTemp {
		l.Entries[topic].Temporary += uint64(size)
	} else {
		l.Entries[topic].Permanent += uint64(size)
	}
}

// SortByPermanentSize sorts the log entries by permanent log size in descending order.
func (l *LogFile) SortByPermanentSize() []*LogEntry {
	result := l.toSlice()
	sort.Sort(logsByPermanentSize(result))
	return result
}

func (l *LogFile) toSlice() []*LogEntry {
	result := make([]*LogEntry, len(l.Entries))
	var i int
	for _, l := range l.Entries {
		result[i] = l
		i++
	}
	return result
}

type logsByPermanentSize []*LogEntry

func (l logsByPermanentSize) Len() int {
	return len(l)
}

func (l logsByPermanentSize) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l logsByPermanentSize) Less(i, j int) bool {
	return l[i].Permanent > l[j].Permanent
}

// LogEntry represents a broker log entry.
type LogEntry struct {
	// Topic the topic.
	Topic string `json:"topic"`
	// Permanent the size of the permanently stored logs in bytes.
	Permanent uint64 `json:"permanent"`
	// Temporary the size of the temporary logs in bytes.
	Temporary uint64 `json:"temporary"`
}
