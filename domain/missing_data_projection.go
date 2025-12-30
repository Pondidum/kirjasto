package domain

import (
	"context"
	"kirjasto/goes"
)

type MissingDataView struct {
	Missing map[int]BookInfo
}

func newMissingDataView() *MissingDataView {
	return &MissingDataView{
		Missing: map[int]BookInfo{},
	}
}

var missingDataProjection = goes.NewEventProjection(
	"missing_data",
	newMissingDataView,
	func(ctx context.Context, view *MissingDataView, event goes.EventDescriptor) error {
		ctx, span := tr.Start(ctx, "project_missing_data")
		defer span.End()

		ev, err := event.Event()
		if err != nil {
			return err
		}

		switch e := ev.(type) {
		case *BookAdded:
			view.Missing[e.BookId] = e.Book

		case *BookImported:
			view.Missing[e.BookId] = e.Book

		case *BookInfoAdded:
			delete(view.Missing, e.BookId)

		}

		return nil
	})
