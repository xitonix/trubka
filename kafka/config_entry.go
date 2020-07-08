package kafka

// ConfigEntry represents a Kafka config.
type ConfigEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ConfigEntriesByName sorts configuration entries by name.
type ConfigEntriesByName []*ConfigEntry

func (c ConfigEntriesByName) Len() int {
	return len(c)
}

func (c ConfigEntriesByName) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ConfigEntriesByName) Less(i, j int) bool {
	return c[i].Name < c[j].Name
}
