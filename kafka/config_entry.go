package kafka

import (
	"fmt"
	"strings"
)

type ConfigEntry struct {
	Name  string
	Value string
}

func (c *ConfigEntry) String() string {
	return fmt.Sprintf("%s: %s", c.Name, c.Value)
}

func (c *ConfigEntry) MultilineValue() string {
	parts := strings.Split(c.Value, ",")
	if len(parts) == 0 {
		return c.Value
	}
	return strings.Join(parts, "\n")
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
