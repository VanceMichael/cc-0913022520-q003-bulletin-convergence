package main

import (
    "log"
    "net/http"
    "os"

    "example.com/disruption-bulletins/internal/api"
)

func main() {
    port := os.Getenv("PORT")
    if port == "" { port = "8080" }
    log.Fatal(http.ListenAndServe(":"+port, api.Router()))
}
