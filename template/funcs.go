package template

import (
	"html/template"
	"strings"
)

var funcs = template.FuncMap(map[string]any{
	"join": func(sep string, v []string) string {
		return strings.Join(v, sep)
	},
})
