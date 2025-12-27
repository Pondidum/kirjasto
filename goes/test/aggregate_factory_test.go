package test

import (
	"kirjasto/domain"
	"kirjasto/goes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFactory(t *testing.T) {

	name, f := goes.FactoryFor(domain.NewLibrary)
	require.Equal(t, "Library", name)

	lib := f()
	require.IsType(t, &domain.Library{}, lib)
}
