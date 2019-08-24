package models

type TopicsByName []Topic

func (t TopicsByName) Len() int {
	return len(t)
}

func (t TopicsByName) Less(i, j int) bool {
	return t[i].Name < t[j].Name
}

func (t TopicsByName) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
