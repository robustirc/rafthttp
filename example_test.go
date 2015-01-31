package rafthttp_test

import (
	"net/http"

	"github.com/robustirc/rafthttp"
)

func Example() {
	transport := rafthttp.NewHTTPTransport(
		rafthttp.DnsAddr("test.example.net:8080"),
		http.DefaultClient,
		nil,
		"")
	http.Handle("/raft/", transport)
	go http.ListenAndServe(":8080", nil)

	// Initialize raft…
}
