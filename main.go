package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	vhost "github.com/inconshreveable/go-vhost"
	"github.com/progrium/qmux/golang/session"
)

type CloseWriter interface {
	CloseWrite() error
}

// Global registry to track active subdomains
type SubdomainRegistry struct {
	mu         sync.RWMutex
	subdomains map[string]*ClientSession
}

type ClientSession struct {
	session    *session.Session
	publicHost string
	listener   net.Listener
}

var registry = &SubdomainRegistry{
	subdomains: make(map[string]*ClientSession),
}

func (sr *SubdomainRegistry) Register(subdomain string, cs *ClientSession) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.subdomains[subdomain] = cs
	log.Printf("Registered subdomain: %s", subdomain)
}

func (sr *SubdomainRegistry) Unregister(subdomain string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if cs, exists := sr.subdomains[subdomain]; exists {
		// Clean up listener
		if cs.listener != nil {
			cs.listener.Close()
		}
		delete(sr.subdomains, subdomain)
		log.Printf("Unregistered subdomain: %s", subdomain)
	}
}

func (sr *SubdomainRegistry) Count() int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.subdomains)
}

func main() {
	var port = flag.String("p", "9999", "server port to use")
	var host = flag.String("h", "start.tunnelsh.top", "server hostname to use")
	var addr = flag.String("b", "127.0.0.1", "ip to bind [server only]")
	flag.Parse()

	// server usage: groktunnel [-h=<hostname>] [-b=<bind ip>]
	l, err := net.Listen("tcp", net.JoinHostPort(*addr, *port))
	fatal(err)
	defer l.Close()
	vmux, err := vhost.NewHTTPMuxer(l, 1*time.Second)
	fatal(err)

	go serve(vmux, *host, *port)

	// Status reporting goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Active subdomains: %d", registry.Count())
		}
	}()

	log.Printf("groktunnel server [%s] ready!\n", *host)
	for {
		conn, err := vmux.NextError()
		if err != nil {
			log.Printf("VMux error: %v", err)
		}
		if conn != nil {
			conn.Close()
		}
	}
}

func serve(vmux *vhost.HTTPMuxer, host, port string) {
	//	ml, err := vmux.Listen(net.JoinHostPort(host, port))
	ml, err := vmux.Listen(host)
	fatal(err)

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleClientConnection(vmux, host, port, w, r)
		}),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("Tunnel endpoint listening on: %s", net.JoinHostPort(host, port))
	srv.Serve(ml)
}

func handleClientConnection(vmux *vhost.HTTPMuxer, host, port string, w http.ResponseWriter, r *http.Request) {

	parts := strings.SplitN(host, ".", 2)

	// Check if the split operation resulted in at least two parts (subdomain and base domain)
	if len(parts) < 2 {
		log.Fatalf("Error: Domain string '%s' does not appear to contain a subdomain.", host)
	}

	// The base domain is the second element after the split
	baseDomain := parts[1]

	// Generate unique subdomain
	//	publicHost := strings.TrimSuffix(net.JoinHostPort(newSubdomain()+baseDomain, port), ":80")
	publicHostOnly := fmt.Sprintf("%s%s", newSubdomain(), baseDomain)

	publicHost := publicHostOnly
	log.Printf("New client requesting subdomain: %s", publicHost)

	// Create listener for this subdomain
	pl, err := vmux.Listen(publicHost)
	if err != nil {
		log.Printf("Failed to create listener for %s: %v", publicHost, err)
		http.Error(w, "Failed to create subdomain", http.StatusInternalServerError)
		return
	}

	// Send response with public host info
	w.Header().Add("X-Public-Host", publicHost)
	w.Header().Add("Connection", "Upgrade")
	w.Header().Set("Upgrade", "tunnelsh/1.0")
	w.WriteHeader(http.StatusOK)

	// Upgrade to raw TCP connection for session multiplexing
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Printf("Failed to hijack connection: %v", err)
		pl.Close()
		return
	}

	// Create session for this client
	sess := session.New(conn)

	// Create client session record
	cs := &ClientSession{
		session:    sess,
		publicHost: publicHost,
		listener:   pl,
	}

	// Register the subdomain
	registry.Register(publicHost, cs)

	// Handle this client in a separate goroutine
	go handleClientSession(cs)
}

func handleClientSession(cs *ClientSession) {
	log.Printf("%s: starting session", cs.publicHost)

	// Ensure cleanup happens
	defer func() {
		log.Printf("%s: ending session", cs.publicHost)
		registry.Unregister(cs.publicHost)
		cs.session.Close()
	}()

	// Handle incoming public requests
	go func() {
		for {
			// Accept connection from public internet
			conn, err := cs.listener.Accept()
			if err != nil {
				log.Printf("%s: listener accept error: %v", cs.publicHost, err)
				return
			}

			log.Printf("%s: handling public request", cs.publicHost)

			// Open channel to client
			ch, err := cs.session.Open(context.Background())
			if err != nil {
				log.Printf("%s: failed to open session channel: %v", cs.publicHost, err)
				conn.Close()
				return
			}
			// Bridge the connections
			go join(ch, conn)
		}
	}()

	// Wait for session to end (client disconnect)
	cs.session.Wait()
}

func join(a io.ReadWriteCloser, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Copy from a to b
	go func() {
		defer wg.Done()
		n, err := io.Copy(b, a)
		if err != nil && err != io.EOF {
			log.Printf("Copy a->b error after %d bytes: %v", n, err)
		}
		// Close write end of b when done reading from a
		if cw, ok := b.(CloseWriter); ok {
			cw.CloseWrite()
		}
	}()

	// Copy from b to a
	go func() {
		defer wg.Done()
		n, err := io.Copy(a, b)
		if err != nil && err != io.EOF {
			log.Printf("Copy b->a error after %d bytes: %v", n, err)
		}
		// Close write end of a when done reading from b
		if cw, ok := a.(CloseWriter); ok {
			cw.CloseWrite()
		}
	}()

	// Wait for both directions to finish
	wg.Wait()

	// Close both connections
	a.Close()
	b.Close()
}

func newSubdomain() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	letters := []rune("abcdefghijklmnopqrstuvwxyz1234567890")
	r := make([]rune, 10)
	for i := range r {
		r[i] = letters[int(b[i])*len(letters)/256]
	}
	return string(r) + "."
}

func fatal(err error) {
	if err != nil {
		log.Printf("Fatal error: %v", err)
		log.Fatal(err)
	}
}
