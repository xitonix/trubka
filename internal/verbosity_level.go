package internal

// VerbosityLevel logging verbosity level.
type VerbosityLevel int8

const (
	// Forced the lowest logging level. Everything will be printed under this level.
	Forced VerbosityLevel = iota
	// Verbose verbose mode (-v)
	Verbose
	// VeryVerbose very verbose mode (-vv)
	VeryVerbose
	// SuperVerbose super verbose mode (-vvv)
	SuperVerbose
	// Chatty extremely verbose mode (-vvvv)
	Chatty
)

// ToVerbosityLevel converts an integer to verbosity level.
func ToVerbosityLevel(counter int) VerbosityLevel {
	switch {
	case counter == 1:
		return Verbose
	case counter == 2:
		return VeryVerbose
	case counter == 3:
		return SuperVerbose
	case counter >= 4:
		return Chatty
	default:
		return Forced
	}
}
