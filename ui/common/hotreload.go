package common

import (
	"crypto/md5"
	"crypto/rand"
	"embed"
	"fmt"
	"kirjasto/tracing"
	"net/http"

	"github.com/gorilla/websocket"
)

//go:embed static/*
var staticFiles embed.FS

func RegisterHandlers(mux *http.ServeMux) {

	upgrader := websocket.Upgrader{}
	b := make([]byte, 4)
	rand.Read(b)
	id := fmt.Sprintf("%x", md5.Sum(b))

	mux.Handle("/static/", http.FileServerFS(staticFiles))

	mux.HandleFunc("/ws/hotreload", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			tracing.ErrorCtx(r.Context(), err)
			return
		}
		defer c.Close()

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				tracing.ErrorCtx(r.Context(), err)
				break
			}

			if err := c.WriteMessage(websocket.TextMessage, []byte(id)); err != nil {
				tracing.ErrorCtx(r.Context(), err)
				break
			}
		}
	})
}
