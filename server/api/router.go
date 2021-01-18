package api

import (
	"net/http"
	"log"
)


type MetaAPIHandlers struct {
	YigFsAPI YigFsLayer
}

type Multiplexer struct {
	handlers map[string]func(w http.ResponseWriter, r *http.Request)
	Handler http.HandlerFunc
}

func NewMultiplexer() *Multiplexer {
	multiplexer := &Multiplexer{
		handlers: make(map[string] func(w http.ResponseWriter, r *http.Request)),
	}
	multiplexer.init()
	return multiplexer
}

func (mux *Multiplexer) init() {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := mux.GetHandler(r.URL.Path)
		if f == nil {
			log.Fatal("unknown path", r.URL.Path)
			return
		}
		f(w,r)
	})
	mux.Handler = handler
}

func (mux *Multiplexer) GetHandler(path string) func(w http.ResponseWriter, r *http.Request) {
	f, ok := mux.handlers[path]
	if ok {
		return f
	}
	return nil
}

func (mux *Multiplexer) HandleFunc(path string, f func(w http.ResponseWriter, r *http.Request)) {
	mux.handlers[path] = f
}
