package internal

import (
	"bytes"
	"strings"

	"github.com/alecthomas/chroma"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

// JsonHighlighter represents a Json highlighter.
type JsonHighlighter struct {
	lex chroma.Lexer
	fm  chroma.Formatter
	st  *chroma.Style
}

// NewJsonHighlighter creates a new instance of a Json highlighter.
func NewJsonHighlighter(style string, enabled bool) *JsonHighlighter {
	if !enabled || strings.EqualFold(style, "none") {
		return &JsonHighlighter{}
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

	return &JsonHighlighter{
		lex: lex,
		fm:  fm,
		st:  st,
	}
}

// Highlight returns the highlighted Json string based on the requested style.
//
// This method does not alter the input if the Highlighter is disabled.
func (j *JsonHighlighter) Highlight(in []byte) []byte {
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
