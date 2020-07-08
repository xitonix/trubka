package list

// List defines a list interface.
type List interface {
	Render()
	AddItem(item interface{})
	AddItemF(format string, a ...interface{})
	Indent()
	UnIndent()
}

// New creates a new list.
func New(plain bool) List {
	if plain {
		return newPlain()
	}
	return newTree()
}
