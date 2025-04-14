package importcmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
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
