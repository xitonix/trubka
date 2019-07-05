package proto

type Finder interface {
	Ls(root string) ([]string, error)
}
