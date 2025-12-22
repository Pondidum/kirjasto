package openlibrary

import (
	"encoding/json"
	"kirjasto/domain"
)

type authorDto struct {
	Key  string
	Name string
}

func authorsFromJson(authorJson string) ([]domain.Author, error) {

	dto := []authorDto{}
	if err := json.Unmarshal([]byte(authorJson), &dto); err != nil {
		return nil, err
	}

	authors := make([]domain.Author, len(dto))
	for i, src := range dto {
		authors[i] = domain.Author{
			Name:     src.Name,
			Source:   "openlibrary",
			SourceId: src.Key,
		}
	}
	return authors, nil
}
