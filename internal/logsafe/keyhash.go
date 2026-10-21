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

// Package logsafe holds helpers for logging without exposing record data.
package logsafe

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// salt is random per process, so a logged hash cannot be reversed by hashing
// guessed keys and cannot be matched across runs or between processes.
var salt = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("cannot read random bytes for the log key hash: %v", err))
	}
	return b
}()

// KeyHash returns a short, salted digest of an object key or position for log
// lines. Two log lines about the same value in one process get the same
// digest, so they can be correlated without writing the value to the log.
// Never log an object key, a position or a payload; log its size and this.
func KeyHash(v []byte) string {
	mac := hmac.New(sha256.New, salt)
	mac.Write(v)
	return hex.EncodeToString(mac.Sum(nil)[:8])
}
