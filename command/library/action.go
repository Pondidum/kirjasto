package library

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/goes"
	"kirjasto/storage"
	"kirjasto/tracing"
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

	library, err := domain.LoadLibrary(ctx, store, domain.LibraryID)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := action(library); err != nil {
		return tracing.Error(span, err)
	}

	if err := goes.Save(ctx, store, library); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
