package list

type List interface {
	SetTitle(title string)
	SetCaption(caption string)
	Render()
	AsTree()
	AddItem(item interface{})
	AddItemF(format string, a ...interface{})
	Intend()
	UnIntend()
}

func New(plain bool) List {
	if plain {
		return NewPlain()
	}
	return NewBullet()
}
