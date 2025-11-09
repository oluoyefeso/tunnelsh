package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
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
	listener   net.Listener // vhost listener for tunnel traffic
	mu         sync.Mutex
	closed     bool
}

var registry = &SubdomainRegistry{
	subdomains: make(map[string]*ClientSession),
}

var tunnelVMux *vhost.HTTPMuxer // Global tunnel vhost muxer

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
		// Close the vhost listener for this subdomain
		if cs.listener != nil {
			cs.listener.Close()
		}
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

func main() {
	var controlPort = flag.String("c", "8443", "control port (direct TLS)")
	var tunnelPort = flag.String("t", "9999", "tunnel port (HTTP from nginx)")
	var host = flag.String("h", "start.tunnelsh.top", "server hostname to use")
	var addr = flag.String("b", "0.0.0.0", "ip to bind")
	var tunnelAddr = flag.String("tunnelAddr", "127.0.0.1", "tunnel ip to bind")
	var certFile = flag.String("cert", "/etc/letsencrypt/live/tunnelsh.top-0001/fullchain.pem", "TLS certificate file")
	var keyFile = flag.String("key", "/etc/letsencrypt/live/tunnelsh.top-0001/privkey.pem", "TLS key file")
	flag.Parse()

	// Status reporting goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Active subdomains: %d", registry.Count())
		}
	}()

	// Start control server (port 8443 - direct TLS with vhost)
	go startControlServer(*addr, *controlPort, *host, *certFile, *keyFile)

	// Start tunnel server (port 9999 - HTTP from nginx)
	startTunnelServer(*tunnelAddr, *tunnelPort, *host)
}

// Control server - handles client connections on port 8443 with direct TLS
func startControlServer(addr, port, host, certFile, keyFile string) {
	log.Printf("Starting control server on %s:%s", addr, port)

	// Load TLS certificate
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load TLS certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Listen with TLS
	l, err := tls.Listen("tcp", net.JoinHostPort(addr, port), tlsConfig)
	if err != nil {
		log.Fatalf("Failed to listen on control port: %v", err)
	}
	defer l.Close()

	// Create vhost muxer on TLS listener
	vmux, err := vhost.NewHTTPMuxer(l, 1*time.Second)
	if err != nil {
		log.Fatalf("Failed to create vhost muxer: %v", err)
	}

	// Listen for control connections on start.tunnelsh.top
	ml, err := vmux.Listen(host)
	if err != nil {
		log.Fatalf("Failed to listen on vhost: %v", err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleClientConnection(host, w, r)
		}),
	}

	log.Printf("Control endpoint ready: https://%s:%s", host, port)

	// Handle vhost errors in background
	go func() {
		for {
			conn, err := vmux.NextError()
			if err != nil {
				log.Printf("VMux error: %v", err)
			}
			if conn != nil {
				conn.Close()
			}
		}
	}()

	if err := srv.Serve(ml); err != nil {
		log.Fatalf("Control server error: %v", err)
	}
}

// Tunnel server - handles public requests on port 9999 (proxied from nginx)
func startTunnelServer(addr, port, host string) {
	log.Printf("Starting tunnel server on %s:%s", addr, port)

	// Listen for raw TCP connections
	l, err := net.Listen("tcp", net.JoinHostPort(addr, port))
	if err != nil {
		log.Fatalf("Failed to listen on tunnel port: %v", err)
	}
	defer l.Close()

	// Create vhost muxer for HTTP-based routing
	vmux, err := vhost.NewHTTPMuxer(l, 1*time.Second)
	if err != nil {
		log.Fatalf("Failed to create tunnel vhost muxer: %v", err)
	}

	// Store globally so handleClientConnection can use it
	tunnelVMux = vmux

	log.Printf("Tunnel endpoint ready on %s (proxied from nginx, using vhost)", net.JoinHostPort(addr, port))

	// Handle vhost errors
	for {
		conn, err := vmux.NextError()
		if err != nil {
			log.Printf("Tunnel VMux error: %v", err)
		}
		if conn != nil {
			conn.Close()
		}
	}
}

// Handle client control connection (on port 8443)
func handleClientConnection(host string, w http.ResponseWriter, r *http.Request) {
	log.Printf("[Control] Client connection from %s", r.RemoteAddr)

	// Check for upgrade header
	if r.Header.Get("Upgrade") != "tunnelsh/1.0" {
		log.Printf("[Control] Missing upgrade header")
		http.Error(w, "Missing upgrade header", http.StatusBadRequest)
		return
	}

	// Extract base domain from control host
	parts := strings.SplitN(host, ".", 2)
	if len(parts) < 2 {
		log.Printf("[Control] Invalid control host: %s", host)
		http.Error(w, "Invalid server configuration", http.StatusInternalServerError)
		return
	}
	baseDomain := parts[1]

	// Generate unique subdomain
	publicHost := fmt.Sprintf("%s%s", newSubdomain(), baseDomain)

	log.Printf("[Control] Assigning subdomain: %s", publicHost)

	// Create vhost listener for this subdomain on the tunnel port
	tunnelListener, err := tunnelVMux.Listen(publicHost)
	if err != nil {
		log.Printf("[Control] Failed to create tunnel listener for %s: %v", publicHost, err)
		http.Error(w, "Failed to create subdomain", http.StatusInternalServerError)
		return
	}

	// Send response with public host info
	w.Header().Set("X-Public-Host", publicHost)
	w.Header().Set("Connection", "Upgrade")
	w.Header().Set("Upgrade", "tunnelsh/1.0")
	w.WriteHeader(http.StatusOK)

	// Hijack connection for session multiplexing
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Printf("[Control] Failed to hijack connection: %v", err)
		tunnelListener.Close()
		return
	}

	// Disable Nagle's algorithm on control connection for lower latency
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			log.Printf("[Control] %s: failed to set TCP_NODELAY: %v", publicHost, err)
		}

		// Tune TCP buffer sizes to reduce kernel-level buffering
		// Smaller buffers force more frequent sends, reducing latency
		const bufferSize = 4096 // 4KB buffers
		if err := tcpConn.SetReadBuffer(bufferSize); err != nil {
			log.Printf("[Control] %s: failed to set read buffer: %v", publicHost, err)
		}
		if err := tcpConn.SetWriteBuffer(bufferSize); err != nil {
			log.Printf("[Control] %s: failed to set write buffer: %v", publicHost, err)
		}
	}

	log.Printf("[Control] %s: connection hijacked, creating session", publicHost)

	// Create session for this client
	sess := session.New(conn)

	// Create client session record
	cs := &ClientSession{
		session:    sess,
		publicHost: publicHost,
		listener:   tunnelListener,
		closed:     false,
	}

	// Register the subdomain
	registry.Register(publicHost, cs)

	log.Printf("[Control] %s: session registered with tunnel listener", publicHost)

	// Handle this client in a separate goroutine
	go handleClientSession(cs)
}

// Handle public tunnel requests (on port 9999 from nginx)
// func handlePublicRequest(w http.ResponseWriter, r *http.Request, hostname string) {
// 	// This function is no longer needed since we're using vhost listeners
// 	// The traffic is handled directly in handleClientSession
// 	log.Printf("[Tunnel] Unexpected direct HTTP request for %s", hostname)
// 	http.Error(w, "Use vhost routing", http.StatusInternalServerError)
// }

func handleClientSession(cs *ClientSession) {
	log.Printf("[Session] %s: starting session", cs.publicHost)

	defer func() {
		log.Printf("[Session] %s: ending session", cs.publicHost)

		cs.mu.Lock()
		cs.closed = true
		cs.mu.Unlock()

		registry.Unregister(cs.publicHost)
		cs.session.Close()
	}()

	// Handle incoming public requests from the vhost listener
	go func() {
		for {
			// Accept connection from nginx via vhost muxer
			conn, err := cs.listener.Accept()
			if err != nil {
				log.Printf("[Session] %s: listener accept error: %v", cs.publicHost, err)
				return
			}

			// Disable Nagle's algorithm to reduce latency for streaming responses
			// This ensures data is sent immediately without TCP-level buffering
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				if err := tcpConn.SetNoDelay(true); err != nil {
					log.Printf("[Session] %s: failed to set TCP_NODELAY: %v", cs.publicHost, err)
				}

				// Tune TCP buffer sizes for lower latency
				// Smaller buffers reduce kernel buffering, especially important for chunked responses
				const bufferSize = 4096 // 4KB buffers
				if err := tcpConn.SetReadBuffer(bufferSize); err != nil {
					log.Printf("[Session] %s: failed to set read buffer: %v", cs.publicHost, err)
				}
				if err := tcpConn.SetWriteBuffer(bufferSize); err != nil {
					log.Printf("[Session] %s: failed to set write buffer: %v", cs.publicHost, err)
				}
			}

			log.Printf("[Session] %s: handling public request", cs.publicHost)

			// Open channel to client through qmux session
			ch, err := cs.session.Open(context.Background())
			if err != nil {
				log.Printf("[Session] %s: failed to open session channel: %v", cs.publicHost, err)
				conn.Close()
				return
			}

			// Bridge the connections (nginx ←→ client)
			go join(ch, conn)
		}
	}()

	// Wait for session to end (client disconnect)
	log.Printf("[Session] %s: waiting for session to end", cs.publicHost)
	cs.session.Wait()
	log.Printf("[Session] %s: session ended - client disconnected", cs.publicHost)
}

// bufConn combines a buffered reader/writer with the underlying connection
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
		if f, ok := bc.Writer.(interface{ Flush() error }); ok {
			f.Flush()
		}
	}
	return n, err
}

func join(a io.ReadWriteCloser, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Copy with explicit flushing to preserve chunked transfer encoding
	copyWithFlush := func(dst io.Writer, src io.Reader, name string) {
		defer wg.Done()

		buf := make([]byte, 32*1024) // 32KB buffer
		totalBytes := int64(0)

		for {
			nr, er := src.Read(buf)
			if nr > 0 {
				totalBytes += int64(nr)
				nw, ew := dst.Write(buf[0:nr])

				// Flush immediately after each write to preserve chunk boundaries
				// This is critical for chunked transfer encoding used by SPA dev servers
				if f, ok := dst.(interface{ Flush() error }); ok {
					if err := f.Flush(); err != nil {
						log.Printf("Copy %s flush error after %d bytes: %v", name, totalBytes, err)
						break
					}
				}

				if ew != nil {
					log.Printf("Copy %s write error after %d bytes: %v", name, totalBytes, ew)
					break
				}
				if nr != nw {
					log.Printf("Copy %s short write: read %d, wrote %d", name, nr, nw)
					break
				}
			}
			if er != nil {
				if er != io.EOF {
					log.Printf("Copy %s read error after %d bytes: %v", name, totalBytes, er)
				}
				break
			}
		}

		// Close write end when done
		if cw, ok := dst.(CloseWriter); ok {
			cw.CloseWrite()
		}
	}

	// Copy from a to b
	go copyWithFlush(b, a, "a->b")

	// Copy from b to a
	go copyWithFlush(a, b, "b->a")

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
