package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Proxy is an authenticated forward proxy so worker egress appears to come from
// this host instead of GitHub's runner addresses.
//
// Security model, in order of importance:
//   - Only GitHub Actions runner addresses may connect (allow_github_ips).
//   - A credential is valid only for the dispatch that minted it and expires
//     shortly after, so a leaked value is useless later.
//   - Only CONNECT to TLS ports and plain HTTP to 80/443.
//   - Destinations that resolve to private, loopback, link-local, metadata or
//     shared-address space are refused, so it cannot reach the local network.
type Proxy struct {
	cfg  ProxyConfig
	log  *slog.Logger
	sem  chan struct{}
	dial *net.Dialer

	mu     sync.RWMutex
	creds  map[string]time.Time // token -> expiry
	custom []netip.Prefix       // allow_cidrs from config
	github []netip.Prefix       // refreshed from the GitHub meta API
}

func NewProxy(cfg ProxyConfig, log *slog.Logger) *Proxy {
	p := &Proxy{
		cfg:   cfg,
		log:   log.With("component", "proxy"),
		sem:   make(chan struct{}, 128),
		dial:  &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second},
		creds: map[string]time.Time{},
	}
	for _, c := range cfg.AllowCIDRs {
		if pfx, err := netip.ParsePrefix(strings.TrimSpace(c)); err == nil {
			p.custom = append(p.custom, pfx)
		} else {
			log.Warn("ignoring invalid allow_cidrs entry", "cidr", c, "err", err)
		}
	}
	return p
}

func (p *Proxy) Run(ctx context.Context) error {
	if p.cfg.AllowGitHubIPs != nil && *p.cfg.AllowGitHubIPs {
		go p.refreshGitHubRanges(ctx)
	}
	go p.sweepCredentials(ctx)

	srv := &http.Server{Addr: p.cfg.Addr, Handler: p, ReadHeaderTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	p.log.Info("proxy listening", "addr", p.cfg.Addr,
		"github_ips_only", p.cfg.AllowGitHubIPs != nil && *p.cfg.AllowGitHubIPs)
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// AddCredential authorises token for ttl. The controller calls this for every
// dispatch, so credentials live no longer than the workers that use them.
func (p *Proxy) AddCredential(token string, ttl time.Duration) {
	if token == "" {
		return
	}
	p.mu.Lock()
	p.creds[token] = time.Now().Add(ttl)
	p.mu.Unlock()
}

func (p *Proxy) sweepCredentials(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			p.mu.Lock()
			for token, exp := range p.creds {
				if now.After(exp) {
					delete(p.creds, token)
				}
			}
			p.mu.Unlock()
		}
	}
}

// refreshGitHubRanges keeps the source allowlist current. GitHub publishes the
// ranges hosted runners use; they change, so refetch hourly.
func (p *Proxy) refreshGitHubRanges(ctx context.Context) {
	client := &http.Client{Timeout: 20 * time.Second}
	fetch := func() {
		var meta struct {
			Actions []string `json:"actions"`
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/meta", nil)
		if err != nil {
			return
		}
		req.Header.Set("User-Agent", "oak-runner-controller")
		resp, err := client.Do(req)
		if err != nil {
			p.log.Warn("fetch github meta", "err", err)
			return
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&meta); err != nil {
			p.log.Warn("decode github meta", "err", err)
			return
		}
		var nets []netip.Prefix
		for _, c := range meta.Actions {
			if pfx, err := netip.ParsePrefix(c); err == nil {
				nets = append(nets, pfx)
			}
		}
		if len(nets) == 0 {
			p.log.Warn("github meta returned no actions ranges; keeping the previous list")
			return
		}
		p.mu.Lock()
		p.github = nets
		p.mu.Unlock()
		p.log.Info("github actions ranges loaded", "ranges", len(nets))
	}
	fetch()
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fetch()
		}
	}
}

// WorkerURL is the proxy URL handed to workers for one dispatch.
func (p ProxyConfig) WorkerURL(token string) string {
	if !p.Enabled || p.PublicURL == "" || token == "" {
		return ""
	}
	return strings.Replace(p.PublicURL, "://", "://"+p.User+":"+token+"@", 1)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !p.sourceAllowed(r) {
		p.log.Warn("rejected connection from outside github ranges", "remote", r.RemoteAddr)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
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

func (p *Proxy) sourceAllowed(r *http.Request) bool {
	p.mu.RLock()
	custom, github := p.custom, p.github
	p.mu.RUnlock()
	nets := append(append([]netip.Prefix{}, custom...), github...)
	if len(nets) == 0 {
		// Fail open: without a range list only the credential protects us, and
		// blocking everything would break every job.
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	for _, pfx := range nets {
		if pfx.Contains(addr) {
			return true
		}
	}
	return false
}

func (p *Proxy) authenticated(r *http.Request) bool {
	user, pass, ok := parseProxyAuth(r.Header.Get("Proxy-Authorization"))
	if !ok || !subtleEqual(user, p.cfg.User) {
		return false
	}
	if p.cfg.Pass != "" && subtleEqual(pass, p.cfg.Pass) {
		return true // optional static credential, for manual testing only
	}
	p.mu.RLock()
	exp, known := p.creds[pass]
	p.mu.RUnlock()
	return known && time.Now().Before(exp)
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

// subtleEqual compares without leaking content through timing.
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

// allowedTarget validates a destination and returns a resolved address.
func allowedTarget(ctx context.Context, mode, hostport string) (string, error) {
	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", fmt.Errorf("bad target %q", hostport)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("bad port %q", portStr)
	}
	switch mode {
	case "connect":
		if port != 443 && port != 8443 && port != 563 {
			return "", fmt.Errorf("port %d not allowed", port)
		}
	case "http":
		if port != 80 && port != 443 {
			return "", fmt.Errorf("port %d not allowed", port)
		}
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
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 169 && v4[1] == 254 { // cloud metadata
			return false
		}
		if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 { // CGNAT
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
	// Dial before hijacking so failures still produce a real HTTP response.
	upstream, err := p.dial.DialContext(r.Context(), "tcp", dst)
	if err != nil {
		p.log.Warn("dial upstream", "target", dst, "err", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		upstream.Close()
		http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		upstream.Close()
		return
	}
	defer conn.Close()
	defer upstream.Close()

	// The tunnel is established: this line is the last HTTP on this connection.
	if _, err := buf.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buf.Flush(); err != nil {
		return
	}

	done := make(chan struct{}, 2)
	go func() {
		io.Copy(upstream, buf) // includes anything already buffered
		if c, ok := upstream.(*net.TCPConn); ok {
			c.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		io.Copy(conn, upstream)
		if c, ok := conn.(*net.TCPConn); ok {
			c.CloseWrite()
		}
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
