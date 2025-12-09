package openlibrary

import "encoding/json"

type Author struct {
	ID   string
	Name string
}

type authorDto struct {
	Key  string
	Name string
}

func authorsFromJson(authorJson string) ([]Author, error) {

	dto := []authorDto{}
	if err := json.Unmarshal([]byte(authorJson), &dto); err != nil {
		return nil, err
	}

	authors := make([]Author, len(dto))
	for i, src := range dto {
		authors[i] = Author{
			ID:   src.Key,
			Name: src.Name,
		}
	}
	return authors, nil
}
