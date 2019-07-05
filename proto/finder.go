package proto

type finder interface {
	ls(root string) ([]string, error)
}
