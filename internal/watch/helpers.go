package watch

import (
	"bytes"
	"strings"
)

func containsString(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

func jsonReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}
