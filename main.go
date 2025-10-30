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
	// listener   net.Listener
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
	if _, exists := sr.subdomains[subdomain]; exists {
		// Clean up listener
		// if cs.listener != nil {
		// 	cs.listener.Close()
		// }
		// cs.session.Close()
		delete(sr.subdomains, subdomain)
		log.Printf("Unregistered subdomain: %s", subdomain)
	}
}

func (sr *SubdomainRegistry) Get(subdomain string) *ClientSession {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.subdomains[subdomain]
}

func (sr *SubdomainRegistry) Count() int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.subdomains)
}

type bufConn struct {
	io.Reader
	io.Writer
	net.Conn
}

func (bc *bufConn) Read(p []byte) (int, error) {
	return bc.Reader.Read(p)
}

func (bc *bufConn) Write(p []byte) (int, error) {
	n, err := bc.Writer.Write(p)
	if err == nil {
		// Flush after each write to ensure data is sent immediately
		if f, ok := bc.Writer.(interface{ Flush() error }); ok {
			f.Flush()
		}
	}
	return n, err
}

func main() {
	var port = flag.String("p", "9999", "server port to use")
	var host = flag.String("h", "start.tunnelsh.top", "server hostname to use")
	var addr = flag.String("b", "127.0.0.1", "ip to bind [server only]")
	flag.Parse()

	// server usage: groktunnel [-h=<hostname>] [-b=<bind ip>]
	// l, err := net.Listen("tcp", net.JoinHostPort(*addr, *port))
	// fatal(err)
	// defer l.Close()
	// vmux, err := vhost.NewHTTPMuxer(l, 1*time.Second)
	// fatal(err)

	// go serve(vmux, *host, *port)

	// Status reporting goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Active subdomains: %d", registry.Count())
		}
	}()

	mux := http.NewServeMux()

	// Route for control connection (client connects here)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, *host)
	})

	srv := &http.Server{
		Addr:    net.JoinHostPort(*addr, *port),
		Handler: mux,
		// No timeouts - we handle long-lived connections
	}

	log.Printf("groktunnel server ready on %s!", srv.Addr)
	log.Printf("Control endpoint: %s", *host)
	log.Fatal(srv.ListenAndServe())
}

func handleRequest(w http.ResponseWriter, r *http.Request, controlHost string) {
	// Get the hostname from the request
	hostname := r.Host

	// Remove port if present
	if idx := strings.Index(hostname, ":"); idx != -1 {
		hostname = hostname[:idx]
	}

	log.Printf("Request for host: %s", hostname)

	// Check if this is a control connection (client registering)
	if hostname == controlHost {
		handleInitialClientConnection(w, r, controlHost)
		return
	}

	// This is a public request to a tunnel subdomain
	handleWildCardClientConnection(w, r, hostname)
}

func handleWildCardClientConnection(w http.ResponseWriter, r *http.Request, hostname string) {
	log.Printf("%s: handling public request", hostname)

	// Find the inital client session for this subdomain
	cs := registry.Get(hostname)
	if cs == nil {
		log.Printf("%s: no active tunnel found", hostname)
		http.Error(w, "Tunnel not found or inactive", http.StatusNotFound)
		return
	}

	ch, err := cs.session.Open(context.Background())
	if err != nil {
		log.Printf("%s: failed to open session channel: %v", hostname, err)
		http.Error(w, "Failed to open tunnel channel", http.StatusBadGateway)
		return
	}
	defer ch.Close()

	// Write the original HTTP request to the tunnel so client receives it
	if err := r.Write(ch); err != nil {
		log.Printf("%s: failed to write request to tunnel: %v", hostname, err)
		http.Error(w, "Failed to forward request", http.StatusBadGateway)
		return
	}

	conn, bufrw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Printf("%s: failed to hijack connection: %v", hostname, err)
		return
	}
	defer conn.Close()

	// Bridge the connections (tunnel response → nginx)
	// Write the original HTTP request to the tunnel
	// The client will receive this and forward to localhost
	if err := r.Write(ch); err != nil {
		log.Printf("%s: failed to write request to tunnel: %v", hostname, err)
		ch.Close()
		conn.Close()
		return
	}

	// Bridge: Copy bidirectionally between tunnel and nginx connection
	// Use the existing join function with buffered connection
	join(ch, &bufConn{bufrw.Reader, bufrw, conn})
}

func handleInitialClientConnection(w http.ResponseWriter, r *http.Request, host string) {

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

	// Send response with public host info
	w.Header().Add("X-Public-Host", publicHost)
	w.Header().Add("Connection", "Upgrade")
	w.Header().Set("Upgrade", "tunnelsh/1.0")
	w.WriteHeader(http.StatusOK)

	// Upgrade to raw TCP connection for session multiplexing
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Printf("Failed to hijack connection: %v", err)
		// pl.Close()
		return
	}

	// Create session for this client
	sess := session.New(conn)

	// Create client session record
	cs := &ClientSession{
		session:    sess,
		publicHost: publicHost,
		// listener:   pl,
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
