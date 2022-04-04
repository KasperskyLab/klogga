package klogga

import (
	"log"
	"os"
)

var host string

// InitHostname call this in your app initialization
// if you want to have host name set automatically on span start
func InitHostname() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("Failed to read hostname:", err)
	}
	host = hostname
}

// SetHostname set custom hostname that will appear on the span from the start
func SetHostname(hn string) {
	host = hn
}
