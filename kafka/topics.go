package kafka

type TopicsByName []Topic

func (t TopicsByName) Len() int {
	return len(t)
}

func (t TopicsByName) Less(i, j int) bool {
	return Name < Name
}

func (t TopicsByName) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
