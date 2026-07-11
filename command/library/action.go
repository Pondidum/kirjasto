package library

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/storage"
	"kirjasto/tracing"
	"strings"
)

func libraryAction(ctx context.Context, config *config.Config, action func(library *domain.Library) error) error {
	ctx, span := tr.Start(ctx, "library_action")
	defer span.End()

	writer, err := storage.Writer(ctx, config.LibraryDb)
	if err != nil {
		return tracing.Error(span, err)
	}

	store, err := domain.NewStore(ctx, writer)
	if err != nil {
		return tracing.Error(span, err)
	}

	library := domain.NewLibrary()
	if store.Load(ctx, domain.LibraryID, library); err != nil {
		return tracing.Error(span, err)
	}

	if err := action(library); err != nil {
		return tracing.Error(span, err)
	}

	if err := store.Save(ctx, library); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func normaliseIsbn(isbn string) string {
	return strings.ReplaceAll(isbn, "-", "")
}
