package gosimlog

import (
	"bytes"
	"fmt"
)

type BitflagChoice struct {
	Mask   int
	Values map[int]string
}

type BitflagValue struct {
	Value int
	Name  string
}

type BitflagFormatter struct {
	Choices []BitflagChoice
	Flags   []BitflagValue
}

func (f *BitflagFormatter) Format(value int) string {
	var buf bytes.Buffer
	for _, choice := range f.Choices {
		masked := value & choice.Mask
		value ^= masked
		if got, ok := choice.Values[masked]; ok {
			if buf.Len() > 0 {
				buf.WriteString("|")
			}
			buf.WriteString(got)
		} else {
			if buf.Len() > 0 {
				buf.WriteString("|")
			}
			fmt.Fprintf(&buf, "%d", masked)
		}
	}
	for _, choice := range f.Flags {
		if value&choice.Value == choice.Value {
			value ^= choice.Value
			if buf.Len() > 0 {
				buf.WriteString("|")
			}
			buf.WriteString(choice.Name)
		}
	}
	if value != 0 {
		if buf.Len() > 0 {
			buf.WriteString("|")
		}
		fmt.Fprintf(&buf, "%d", value)
	}
	return buf.String()
}
