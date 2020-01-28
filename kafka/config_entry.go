package kafka

type ConfigEntry struct {
	Name  string
	Value string
}

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
