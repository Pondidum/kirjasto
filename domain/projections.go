package domain

import "kirjasto/goes"

func RegisterProjections(store *goes.SqliteStore) error {

	projection := NewLibraryProjection()
	if err := store.RegisterProjection("library_view", projection); err != nil {
		return err
	}

	return nil
}
