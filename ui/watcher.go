package ui

import (
	"context"
	"fmt"
	"kirjasto/tracing"

	"github.com/fsnotify/fsnotify"
)

func StartWatching(ctx context.Context, paths []string, actions ...func(path string) error) error {
	ctx, span := tr.Start(ctx, "new_watcher")
	defer span.End()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return tracing.Error(span, err)
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				fmt.Println("-->", event.Name, "changed, triggering reload")

				for _, action := range actions {
					action(event.Name)
				}

				// do things
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				tracing.Error(span, err)
			}
		}
	}()

	for _, path := range paths {
		if err := watcher.Add(path); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}
