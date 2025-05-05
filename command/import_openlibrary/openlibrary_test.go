package import_openlibrary

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratingFile(t *testing.T) {

	csv, err := os.Open("test_data/authors.csv")
	require.NoError(t, err)
	defer csv.Close()

	count := 0
	for content, err := range iterateFile(csv) {
		require.NoError(t, err)
		count++

		dto := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(content), &dto))

		require.Contains(t, dto, "key")
	}
	require.Equal(t, 10, count)
}
