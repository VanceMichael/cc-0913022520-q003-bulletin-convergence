package api

import (
    "encoding/json"
    "net/http"
    "github.com/go-chi/chi/v5"
)

const serviceName = "disruption-bulletins"

func Router() http.Handler {
    router := chi.NewRouter()
	router.Get("/health", health(serviceName))
    return router
}

func health(service string) http.HandlerFunc {
    return func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": service})
    }
}
