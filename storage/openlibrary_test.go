package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindingBooks(t *testing.T) {
	ctx := context.Background()
	reader, err := Reader(ctx, "../dev.sqlite")
	require.NoError(t, err)

	books, err := FindBooks(ctx, reader, "rogue heroes")
	require.NoError(t, err)
	require.Equal(t, 8, len(books))
	require.NotEmpty(t, books[0].Editions)

	i := slices.IndexFunc(books, func(b Book) bool { return b.ID == "OL20036147W" })
	book := books[i]
	require.Equal(t, "OL20036147W", book.ID)
	require.Len(t, book.Editions, 7)

}
