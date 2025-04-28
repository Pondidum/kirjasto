package ui

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"kirjasto/config"
	"kirjasto/template"
	"kirjasto/tracing"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func HotReload(fs template.FS) *hotReload {
	return &hotReload{
		fs: fs,
	}
}

type hotReload struct {
	fs    template.FS
	token []byte
}

func (hr *hotReload) Register(ctx context.Context, cfg *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {

	hr.createToken()

	upgrader := websocket.Upgrader{}
	mux.HandleFunc("GET /ws/hotreload", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			tracing.ErrorCtx(r.Context(), err)
			return
		}
		defer c.Close()

		for {
			if err := c.WriteMessage(websocket.TextMessage, hr.token); err != nil {
				tracing.ErrorCtx(r.Context(), err)
				break
			}
			time.Sleep(1 * time.Second)
		}
	})

	return nil
}

func (hr *hotReload) createToken() {
	b := make([]byte, 20)
	rand.Read(b)
	hr.token = fmt.Appendf(nil, "%x", md5.Sum(b))
}

func (hr *hotReload) Reload(ctx context.Context) error {
	hr.createToken()
	return nil
}
