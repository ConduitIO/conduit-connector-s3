// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partitioner

import (
	"fmt"
	"strings"
	"time"
)

// PathFormat is a parsed Kafka Connect `path.format` (Joda DateTimeFormat
// syntax), restricted to numeric fields.
//
// Supported: yyyy and YYYY (4-digit year), MM or M (month), dd or d (day of
// month), HH or H (hour 0-23), mm or m (minute), ss or s (second); a doubled
// letter is zero padded to 2 digits. Text in single quotes is literal ('year'),
// and a doubled single quote stands for one quote. Any other character outside
// quotes that is not a letter is literal. Every other letter is rejected: text fields (MMM, EEE) would
// depend on Kafka Connect's `locale`, and week-based or fractional fields have
// no use in a directory layout.
type PathFormat struct {
	parts []pathPart
}

type pathPart struct {
	lit   string
	field byte // 'y', 'M', 'd', 'H', 'm', 's'; 0 for a literal
	width int
}

// ParsePathFormat parses a Kafka Connect path.format. Errors wrap ErrConfig.
func ParsePathFormat(s string) (PathFormat, error) {
	var pf PathFormat
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			pf.parts = append(pf.parts, pathPart{lit: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				lit.WriteByte('\'')
				i += 2
				continue
			}
			// quoted text; inside it '' is a quote, a single ' ends it
			j, closed := i+1, false
			for j < len(s) {
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						lit.WriteByte('\'')
						j += 2
						continue
					}
					closed = true
					break
				}
				lit.WriteByte(s[j])
				j++
			}
			if !closed {
				return PathFormat{}, configErr("partitioner.pathFormat", "unterminated quote in %q", s)
			}
			i = j + 1
		case isLetter(c):
			n := 1
			for i+n < len(s) && s[i+n] == c {
				n++
			}
			field, width, err := fieldOf(c, n)
			if err != nil {
				return PathFormat{}, configErr("partitioner.pathFormat", "%q in %q: %v; supported are yyyy (or YYYY), MM, dd, HH, mm, ss (single letters unpadded), quoted text ('year') and other characters as literals", s[i:i+n], s, err)
			}
			flush()
			pf.parts = append(pf.parts, pathPart{field: field, width: width})
			i += n
		default:
			lit.WriteByte(c)
			i++
		}
	}
	flush()
	if len(pf.parts) == 0 {
		return PathFormat{}, configErr("partitioner.pathFormat", "the pattern is empty")
	}
	// Every directory level must be a real name: an empty, "." or ".."
	// segment would be removed or resolved by path.Join when the key is built,
	// changing the layout or leaving the prefix. Time fields render as digits,
	// so one sample shows the structure of every rendering.
	for _, seg := range strings.Split(pf.Format(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return PathFormat{}, configErr("partitioner.pathFormat", "%q has an empty, \".\" or \"..\" directory level (no leading, trailing or doubled /)", s)
		}
	}
	return pf, nil
}

func isLetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

func fieldOf(c byte, n int) (field byte, width int, err error) {
	switch c {
	case 'y', 'Y':
		if n != 4 {
			return 0, 0, fmt.Errorf("only a 4-letter year is supported")
		}
		return 'y', 4, nil
	case 'M', 'd', 'H', 'm', 's':
		if n > 2 {
			return 0, 0, fmt.Errorf("text and wider numeric fields are not supported")
		}
		return c, n, nil
	}
	return 0, 0, fmt.Errorf("unsupported pattern letter %q", c)
}

// Format renders t (already in the partition time zone).
func (pf PathFormat) Format(t time.Time) string {
	var b strings.Builder
	for _, p := range pf.parts {
		if p.field == 0 {
			b.WriteString(p.lit)
			continue
		}
		var v int
		switch p.field {
		case 'y':
			v = t.Year()
		case 'M':
			v = int(t.Month())
		case 'd':
			v = t.Day()
		case 'H':
			v = t.Hour()
		case 'm':
			v = t.Minute()
		case 's':
			v = t.Second()
		}
		fmt.Fprintf(&b, "%0*d", p.width, v)
	}
	return b.String()
}
