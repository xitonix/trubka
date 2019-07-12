package internal

type VerbosityLevel int8

const (
	Quiet VerbosityLevel = iota
	Normal
	Verbose
	SuperVerbose
)

func ToVerbosityLevel(counter int) VerbosityLevel {
	switch {
	case counter == 1:
		return Normal
	case counter == 2:
		return Verbose
	case counter >= 3:
		return SuperVerbose
	default:
		return Quiet
	}
}
