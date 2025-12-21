package storage

import "strings"

func FtsEscape(value string) string {
	return `"` + strings.ReplaceAll(value, ":", `" "`) + `"`
}
