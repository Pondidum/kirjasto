package domain

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
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
