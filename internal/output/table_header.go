package output

type TableHeader struct {
	Key       string
	Alignment int
}

func H(key string, alignment int) TableHeader {
	return TableHeader{
		Key:       key,
		Alignment: alignment,
	}
}
