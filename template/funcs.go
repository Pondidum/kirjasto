package template

import (
	"fmt"
	"html/template"
	"strings"
)

var funcs = template.FuncMap(map[string]any{
	"join": func(sep string, v []string) string {
		return strings.Join(v, sep)
	},
	"ternary": func(condition bool, values ...string) string {
		if condition {
			return values[0]
		}
		if len(values) > 1 {
			return values[1]
		}

		return ""
	},
	"dict": func(values ...any) map[string]any {
		dict := map[string]any{}
		total := len(values)

		for i := 0; i < total; i += 2 {
			key := fmt.Sprintf("%v", values[i])
			if i+1 >= total {
				dict[key] = ""
				continue
			}
			dict[key] = values[i+1]
		}
		return dict
	},
})
