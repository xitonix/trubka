package internal

// VerbosityLevel logging verbosity level.
type VerbosityLevel int8

const (
	// Quiet the lowest logging level. Everything will be printer under this level.
	Quiet VerbosityLevel = iota
	// Normal normal mode (-v)
	Normal
	// Verbose verbose mode (-vv)
	Verbose
	// SuperVerbose super verbose mode (-vvv)
	SuperVerbose
)

// ToVerbosityLevel converts an integer to verbosity level.
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
