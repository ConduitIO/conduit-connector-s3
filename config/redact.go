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
)

// userinfoPattern finds the user info of a URL that url.Parse rejected.
var userinfoPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/?#\s"'` + "`" + `]*@`)

// redactURL returns raw with the user info (a user name and password written
// as scheme://user:password@host) replaced by a placeholder, so that a URL can
// be named in an error or a log line without its credentials. A URL without
// user info is returned unchanged.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// The URL is not valid, so it can still carry credentials: cut the
		// user info out textually.
		return userinfoPattern.ReplaceAllString(raw, "${1}redacted:xxxxx@")
	}
	if u.User == nil {
		return raw
	}
	// Redacted masks the password; the user name is replaced as well because
	// it is often the secret (a token used as the user name).
	u.User = url.UserPassword("redacted", "")
	return u.Redacted()
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
