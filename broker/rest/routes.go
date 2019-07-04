package rest

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/eleme/lindb/config"

	rice "github.com/GeertJohan/go.rice"

	"github.com/gorilla/mux"
)

type route struct {
	name    string
	method  string
	pattern string
	handler http.HandlerFunc
}

type routes []route

var rs = routes{
	route{"CreateOrUpdateDatabase", "POST", "/database", CreateOrUpdateDatabase},
	route{"GetDatabase", "Get", "/database", GetDatabase},
}

// NewRouter returns a new router with a panic handler and a static server
// handler.
func NewRouter(config *config.BrokerConfig) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	for _, route := range rs {
		router.
			Methods(route.method).
			Path(route.pattern).
			Name(route.name).
			Handler(panicHandler(route.handler))
	}
	// static server path
	router.PathPrefix("/static/").
		Handler(http.StripPrefix("/static/",
			http.FileServer(rice.MustFindBox("./../../web/build").HTTPBox())))
	return router
}

// panicHandler handles panics and returns a json response with error message
// and http code 500
func panicHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer func() {
			r := recover()
			if r != nil {
				fmt.Println("come in")
				switch t := r.(type) {
				case string:
					err = errors.New(t)
				case error:
					err = t
				default:
					err = errors.New("UnKnow ERROR")
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}()
		h.ServeHTTP(w, r)
	})
}
