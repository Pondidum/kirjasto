package routing

import "net/http"

func Form(r *http.Request) (map[string]string, error) {

	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	values := map[string]string{}

	for key, vals := range r.Form {
		if len(vals) > 0 {
			values[key] = vals[0]
		}
	}

	return values, nil
}
