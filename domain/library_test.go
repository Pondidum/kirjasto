package domain

import (
	"kirjasto/goes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsbnSorting(t *testing.T) {
	isbns := []string{
		"0107717190",
		"9780107717193",
		"0107717204",
	}

	slices.SortFunc(isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	assert.Equal(t, []string{
		"9780107717193",
		"0107717190",
		"0107717204",
	}, isbns)
}

func TestAddingBooks(t *testing.T) {
	l := CreateLibrary(LibraryID)

	id1, err := l.ImportBook(BookInfo{}, ImportData{})
	require.NoError(t, err)
	require.Equal(t, 0, id1)

	id2, err := l.ImportBook(BookInfo{}, ImportData{})
	require.NoError(t, err)
	require.Equal(t, 1, id2)

	store := goes.NewMemoryStore()
	err = store.Save(t.Context(), l)
	require.NoError(t, err)

	events := store.Aggregates[LibraryID]
	require.Len(t, events, 3)
}
