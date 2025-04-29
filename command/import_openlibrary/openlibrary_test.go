package import_openlibrary

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkDeserialization(t *testing.T) {

	t.Run("normal author structure", func(t *testing.T) {
		dto := &workDto{}
		assert.NoError(t, json.Unmarshal([]byte(workNormalAuthor), &dto))
		assert.Equal(t, []authorLink{
			authorLink{Key: "/authors/someone"},
		}, dto.Authors)
	})
	t.Run("string based author", func(t *testing.T) {
		dto := &workDto{}
		assert.NoError(t, json.Unmarshal([]byte(workStringAuthor), &dto))
		assert.Equal(t, []authorLink{
			authorLink{Key: "/authors/someone"},
		}, dto.Authors)
	})
}

var workNormalAuthor = `{ "key": "/works/normal-author", "authors": [{"type": "/type/author_role", "author": {"key": "/authors/someone"}}]}`
var workStringAuthor = `{ "key": "/works/normal-author",  "authors": [{"type": {"key": "/type/author_role"}, "author": "/authors/someone"}]}`

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

func TestIteratingRecords(t *testing.T) {

	csv, err := os.Open("test_data/authors.csv")
	require.NoError(t, err)
	defer csv.Close()

	count := 0
	for record, err := range iterateRecords[Record](csv) {
		require.NoError(t, err)
		count++

		require.NotEmpty(t, record.Key)
	}
	require.Equal(t, 10, count)
}
