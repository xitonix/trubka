package internal

import (
	"bytes"
	"strings"

	"github.com/alecthomas/chroma"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

// JSONHighlighter represents a Json highlighter.
type JSONHighlighter struct {
	lex chroma.Lexer
	fm  chroma.Formatter
	st  *chroma.Style
}

// NewJSONHighlighter creates a new instance of a Json highlighter.
func NewJSONHighlighter(style string, enabled bool) *JSONHighlighter {
	if !enabled || strings.EqualFold(style, "none") {
		return &JSONHighlighter{}
	}
	lex := lexers.Get("json")
	fm := formatters.Get("terminal")
	if fm == nil {
		fm = formatters.Fallback
	}

	st := styles.Get(style)
	if st == nil {
		st = styles.Fallback
	}

	return &JSONHighlighter{
		lex: lex,
		fm:  fm,
		st:  st,
	}
}

// Highlight returns the highlighted Json string based on the requested style.
//
// This method does not alter the input if the Highlighter is disabled.
func (j *JSONHighlighter) Highlight(in []byte) []byte {
	if j.lex == nil {
		return in
	}
	tokens, err := j.lex.Tokenise(nil, string(in))
	if err != nil {
		return in
	}
	var buf bytes.Buffer
	err = j.fm.Format(&buf, j.st, tokens)
	if err != nil {
		return in
	}
	return buf.Bytes()
}
