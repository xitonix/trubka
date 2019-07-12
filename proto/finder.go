package proto

type finder interface {
	ls() ([]string, error)
}
