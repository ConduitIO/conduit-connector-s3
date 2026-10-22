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
	u, err := url.Parse(raw)
	switch {
	case err == nil && u.User != nil && u.Scheme != "" && u.Host != "" && strings.Count(raw, "@") == 1:
		u.User = url.UserPassword("redacted", "")
		s = u.Redacted()
	case strings.Contains(raw, "@"):
		// A password with an unencoded '/', '?' or '#' (also when the URL
		// parses and keeps the tail of the password in its path), a URL without
		// a scheme or one that does not parse: url.Parse does not find the user
		// info, or finds only part of it. More than one '@' is read the same
		// way. Cut everything up to the last '@'. This can
		// hide more than the credentials (an '@' in a path), which is the safe
		// side of the trade.
		at := strings.LastIndex(raw, "@")
		s = schemePrefix.FindString(raw) + "redacted:xxxxx@" + raw[at+1:]
	}
	return maskQueryAndFragment(s)
}

// schemePrefix matches an optional "scheme://" at the start of a URL.
var schemePrefix = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)

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

// parseCause returns what is safe to say about why a URL did not parse. The
// url.Error text quotes parts of the URL (the port, an escape) and the URL may
// be broken in a way that hides its user info, so it is not shown.
func parseCause(error) error {
	return errors.New("the URL cannot be parsed")
}
