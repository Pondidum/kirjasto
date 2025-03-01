package common

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"embed"
	"fmt"
	"kirjasto/tracing"
	"net/http"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tr = otel.Tracer("ui.common")

//go:embed static/*
var staticFiles embed.FS

const staticFilesDir string = "./ui/common/static/"

func RegisterHandlers(ctx context.Context, mux *http.ServeMux) error {
	ctx, span := tr.Start(ctx, "register_handlers")
	defer span.End()

	hasLocalFiles := true
	if _, err := os.Stat(staticFilesDir); os.IsNotExist(err) {
		hasLocalFiles = false
	}

	span.SetAttributes(attribute.Bool("static.has_local_files", hasLocalFiles))

	if !hasLocalFiles {
		mux.Handle("/static/", http.FileServerFS(staticFiles))
		return nil
	}

	if err := addHotReload(ctx, mux); err != nil {
		return err
	}

	return nil

}

func addHotReload(ctx context.Context, mux *http.ServeMux) error {
	ctx, span := tr.Start(ctx, "hot_reload")
	defer span.End()

	mux.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir(staticFilesDir))))

	token := hotreloadToken()

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

				fmt.Printf("--> %s changed, triggering hot reload\n", event.Name)
				token = hotreloadToken()
				// do things
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				tracing.Error(span, err)
			}
		}
	}()

	if err := watcher.Add(staticFilesDir); err != nil {
		return tracing.Error(span, err)
	}

	upgrader := websocket.Upgrader{}

	mux.HandleFunc("/ws/hotreload", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			tracing.ErrorCtx(r.Context(), err)
			return
		}
		defer c.Close()

		for {
			if err := c.WriteMessage(websocket.TextMessage, token); err != nil {
				tracing.ErrorCtx(r.Context(), err)
				break
			}
			time.Sleep(1 * time.Second)
		}
	})

	return nil
}

func hotreloadToken() []byte {
	b := make([]byte, 20)
	rand.Read(b)
	return fmt.Appendf(nil, "%x", md5.Sum(b))
}
