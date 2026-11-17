package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Proxy is a small authenticated forward proxy so worker egress appears to come
// from this host instead of GitHub's runner addresses. It handles CONNECT
// tunnels (HTTPS) and plain HTTP forwarding, and refuses to reach private or
// loopback addresses so it cannot be used to poke at the local network.
type Proxy struct {
	cfg  ProxyConfig
	log  *slog.Logger
	sem  chan struct{}
	dial *net.Dialer
}

func NewProxy(cfg ProxyConfig, log *slog.Logger) *Proxy {
	return &Proxy{
		cfg:  cfg,
		log:  log.With("component", "proxy"),
		sem:  make(chan struct{}, 64),
		dial: &net.Dialer{Timeout: 30 * time.Second},
	}
}

func (p *Proxy) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              p.cfg.Addr,
		Handler:           p,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	p.log.Info("proxy listening", "addr", p.cfg.Addr)
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// WorkerURL is the proxy URL handed to workers, credentials included.
func (p ProxyConfig) WorkerURL() string {
	if !p.Enabled || p.PublicURL == "" {
		return ""
	}
	if p.User == "" {
		return p.PublicURL
	}
	u := strings.Replace(p.PublicURL, "://", "://"+p.User+":"+p.Pass+"@", 1)
	return u
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !p.authenticated(r) {
		w.Header().Set("Proxy-Authenticate", `Basic realm="oak"`)
		http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
		return
	}
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	default:
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	if r.Method == http.MethodConnect {
		p.tunnel(w, r)
		return
	}
	p.forward(w, r)
}

func (p *Proxy) authenticated(r *http.Request) bool {
	if p.cfg.User == "" {
		return false // never run credential-less
	}
	user, pass, ok := parseProxyAuth(r.Header.Get("Proxy-Authorization"))
	if !ok {
		return false
	}
	return subtleEqual(user, p.cfg.User) && subtleEqual(pass, p.cfg.Pass)
}

func parseProxyAuth(h string) (user, pass string, ok bool) {
	const prefix = "basic "
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(h[len(prefix):]))
	if err != nil {
		return "", "", false
	}
	user, pass, ok = strings.Cut(string(raw), ":")
	return user, pass, ok
}

// subtleEqual compares without leaking length or content through timing.
func subtleEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// allowedTarget rejects destinations that could reach the local network.
func allowedTarget(ctx context.Context, network, hostport string) (string, error) {
	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", fmt.Errorf("bad target %q: %w", hostport, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("bad port %q", portStr)
	}
	// CONNECT is only meaningful for TLS ports plus the usual alt ports.
	if network == "connect" && port != 443 && port != 8443 && port != 563 {
		return "", fmt.Errorf("port %d not allowed", port)
	}
	if network == "http" && port != 80 && port != 443 {
		return "", fmt.Errorf("port %d not allowed", port)
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", errors.New("no addresses")
	}
	for _, ip := range ips {
		if !publicIP(ip.IP) {
			return "", fmt.Errorf("destination %s resolves to a non-public address", host)
		}
	}
	return net.JoinHostPort(ips[0].IP.String(), portStr), nil
}

func publicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	// Never allow the cloud metadata service or the CGNAT range.
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 169 && v4[1] == 254 {
			return false
		}
		if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return false
		}
	}
	return true
}

func (p *Proxy) tunnel(w http.ResponseWriter, r *http.Request) {
	dst, err := allowedTarget(r.Context(), "connect", r.Host)
	if err != nil {
		p.log.Warn("rejected tunnel", "target", r.Host, "err", err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	upstream, err := p.dial.DialContext(r.Context(), "tcp", dst)
	if err != nil {
		p.log.Warn("dial upstream", "target", dst, "err", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	flusher.Flush()

	down, ok := w.(io.ReadWriteCloser)
	if !ok {
		if hj, ok := w.(http.Hijacker); ok {
			conn, buf, err := hj.Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			pipe(conn, upstream, buf)
			return
		}
		return
	}
	pipe(down, upstream, nil)
}

func pipe(client io.ReadWriteCloser, upstream io.ReadWriteCloser, buf *bufio.ReadWriter) {
	done := make(chan struct{}, 2)
	go func() {
		if buf != nil && buf.Reader.Buffered() > 0 {
			io.CopyN(upstream, buf, int64(buf.Reader.Buffered()))
		}
		io.Copy(upstream, client)
		upstream.Close()
		done <- struct{}{}
	}()
	go func() {
		io.Copy(client, upstream)
		client.Close()
		done <- struct{}{}
	}()
	<-done
}

// forward handles plain HTTP requests.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil || r.URL.Host == "" {
		http.Error(w, "absolute URL required", http.StatusBadRequest)
		return
	}
	port := r.URL.Port()
	if port == "" {
		port = "80"
	}
	dst, err := allowedTarget(r.Context(), "http", net.JoinHostPort(r.URL.Hostname(), port))
	if err != nil {
		p.log.Warn("rejected request", "target", r.URL.Host, "err", err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL.Scheme = "http"
	out.URL.Host = dst
	out.Header.Del("Proxy-Authorization")
	out.Header.Del("Proxy-Connection")
	out.Header.Del("Connection")

	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	resp, err := transport.RoundTrip(out)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
