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

package config

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// userinfoPattern finds the user info of a URL that url.Parse rejected.
var userinfoPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/?#\s"'` + "`" + `]*@`)

// redactURL returns raw with its credentials replaced, so that a URL can be
// named in an error or a log line:
//   - the user info (scheme://user:password@host); the user name is replaced
//     as well because it is often the secret (a token used as the user name);
//   - every query parameter value and the fragment, whatever the parameter is
//     called, because query credentials (token, key, sig, X-Amz-Signature, ...)
//     come under too many names to match; the names are kept.
//
// A URL without user info, query or fragment is returned unchanged. A URL
// that url.Parse rejects can still carry credentials, so it is redacted
// textually.
func redactURL(raw string) string {
	s := raw
	if u, err := url.Parse(raw); err != nil {
		s = userinfoPattern.ReplaceAllString(raw, "${1}redacted:xxxxx@")
	} else if u.User != nil {
		u.User = url.UserPassword("redacted", "")
		s = u.Redacted()
	}
	return maskQueryAndFragment(s)
}

// maskQueryAndFragment masks the value of every query parameter and the
// fragment, working on the text so that it also covers URLs that do not parse.
func maskQueryAndFragment(s string) string {
	i := strings.IndexAny(s, "?#")
	if i < 0 {
		return s
	}
	head, tail := s[:i], s[i:]
	var query, fragment string
	if tail[0] == '?' {
		query = tail[1:]
		if j := strings.IndexByte(query, '#'); j >= 0 {
			query, fragment = query[:j], query[j+1:]
		}
	} else {
		fragment = tail[1:]
	}

	var b strings.Builder
	b.WriteString(head)
	if tail[0] == '?' {
		b.WriteByte('?')
		sep := "&"
		if strings.Contains(query, ";") && !strings.Contains(query, "&") {
			sep = ";"
		}
		parts := strings.FieldsFunc(query, func(r rune) bool { return r == '&' || r == ';' })
		for k, part := range parts {
			if k > 0 {
				b.WriteString(sep)
			}
			if eq := strings.IndexByte(part, '='); eq >= 0 {
				b.WriteString(part[:eq+1])
			}
			b.WriteString("xxxxx")
		}
	}
	if fragment != "" {
		b.WriteString("#xxxxx")
	}
	return b.String()
}

// parseCause returns the reason url.Parse rejected a URL without the URL
// itself, which url.Error repeats in its message, user info included.
func parseCause(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
