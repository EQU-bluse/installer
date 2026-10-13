package handler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeGitHub struct {
	mu          sync.Mutex
	tag         string
	releaseHits int
	sumHits     int
	sums        bool
	sumsFail    bool
	block       chan struct{}
	started     chan struct{}
	ioCanceled  chan struct{}
	srv         *httptest.Server
}

func newFakeGitHub(t *testing.T) *fakeGitHub {
	t.Helper()
	f := &fakeGitHub{
		tag:        "v1",
		started:    make(chan struct{}, 256),
		ioCanceled: make(chan struct{}, 256),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/SHASUMS256.txt":
			f.mu.Lock()
			fail := f.sumsFail
			f.sumHits++
			f.mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			for _, name := range fakeBinaryAssets(f.srv.URL) {
				fmt.Fprintf(w, "%x  %s\n", sha256.Sum256([]byte(name["name"].(string))), name["name"])
			}
		case strings.Contains(r.URL.Path, "/releases/"):
			f.mu.Lock()
			block := f.block
			f.mu.Unlock()
			select {
			case f.started <- struct{}{}:
			default:
			}
			if block != nil {
				select {
				case <-block:
				case <-r.Context().Done():
					select {
					case f.ioCanceled <- struct{}{}:
					default:
					}
					return
				}
			}
			f.mu.Lock()
			tag := f.tag
			f.releaseHits++
			sums := f.sums
			f.mu.Unlock()
			assets := fakeBinaryAssets(f.srv.URL)
			if sums {
				assets = append(assets, map[string]any{
					"name":                 "SHASUMS256.txt",
					"browser_download_url": f.srv.URL + "/SHASUMS256.txt",
					"size":                 2048,
				})
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"tag_name": tag,
				"assets":   assets,
			})
		default:
			http.NotFound(w, r)
		}
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func fakeBinaryAssets(base string) []map[string]any {
	return []map[string]any{
		{
			"name":                 "tool-linux-amd64.tar.gz",
			"browser_download_url": base + "/d/tool-linux-amd64.tar.gz",
			"size":                 5 * 1024 * 1024,
		},
		{
			"name":                 "tool-darwin-arm64.tar.gz",
			"browser_download_url": base + "/d/tool-darwin-arm64.tar.gz",
			"size":                 5 * 1024 * 1024,
		},
	}
}

func (f *fakeGitHub) handler() *Handler {
	return &Handler{Client: f.srv.Client(), GHAPI: f.srv.URL}
}

func (f *fakeGitHub) counts() (releases, sums int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseHits, f.sumHits
}

func (f *fakeGitHub) setTag(tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tag = tag
}

func (f *fakeGitHub) setBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = ch
}

func (f *fakeGitHub) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("fake GitHub release request never started")
	}
}

func newTestResolver(t *testing.T, h *Handler) (*resolver, *manualClock) {
	t.Helper()
	clk := &manualClock{t: time.Unix(1_700_000_000, 0)}
	r := h.resolver()
	r.now = clk.now
	return r, clk
}

func waitWaiters(t *testing.T, r *resolver, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		f := r.flights[key]
		got := 0
		if f != nil {
			got = f.waiters
		}
		r.mu.Unlock()
		if got == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %d waiters on %s", want, key)
}

func assetByName(result QueryResult, name string) Asset {
	for _, a := range result.Assets {
		if a.Name == name {
			return a
		}
	}
	return Asset{}
}

func latestQuery(selectAsset string) Query {
	return Query{User: "acme", Program: "tool", Release: "latest", Select: selectAsset}
}

func TestResolverConcurrentExpirySharesFlight(t *testing.T) {
	f := newFakeGitHub(t)
	f.sums = true
	h := f.handler()
	r, clk := newTestResolver(t, h)
	q := latestQuery("")

	first, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if first.ResolvedRelease != "v1" || assetByName(first, "tool-linux-amd64.tar.gz").SHA256 == "" {
		t.Fatalf("unexpected initial result: %#v", first)
	}

	f.setTag("v2")
	clk.add(cacheTTL + time.Minute)

	gate := make(chan struct{})
	f.setBlock(gate)

	const n = 20
	start := make(chan struct{})
	results := make([]QueryResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = h.execute(context.Background(), q)
		}(i)
	}
	close(start)
	f.waitStarted(t)
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("waiter %d failed: %s", i, err)
		}
		if results[i].ResolvedRelease != "v2" {
			t.Fatalf("waiter %d resolved %s", i, results[i].ResolvedRelease)
		}
		if assetByName(results[i], "tool-linux-amd64.tar.gz").SHA256 == "" {
			t.Fatalf("waiter %d missing checksum", i)
		}
	}
	if releases, sums := f.counts(); releases != 2 || sums != 2 {
		t.Fatalf("expected 2 release and 2 checksum fetches total, got %d/%d", releases, sums)
	}
	if r.flights[keyFor(q)] != nil {
		t.Fatal("flight was not retired")
	}
}

func TestResolverDifferentSelectorsDoNotShare(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	linux := latestQuery("linux")
	darwin := latestQuery("darwin")
	gate := make(chan struct{})
	f.setBlock(gate)

	var wg sync.WaitGroup
	wg.Add(2)
	var rl, rd QueryResult
	go func() { defer wg.Done(); rl, _ = h.execute(context.Background(), linux) }()
	go func() { defer wg.Done(); rd, _ = h.execute(context.Background(), darwin) }()
	f.waitStarted(t)
	f.waitStarted(t)
	close(gate)
	wg.Wait()

	if releases, _ := f.counts(); releases != 2 {
		t.Fatalf("expected 2 fetches for distinct selects, got %d", releases)
	}
	if len(rl.Assets) != 1 || rl.Assets[0].Name != "tool-linux-amd64.tar.gz" {
		t.Fatalf("unexpected linux assets: %#v", rl.Assets)
	}
	if len(rd.Assets) != 1 || rd.Assets[0].Name != "tool-darwin-arm64.tar.gz" {
		t.Fatalf("unexpected darwin assets: %#v", rd.Assets)
	}

	render := linux
	render.OS = "darwin"
	render.Arch = "arm64"
	render.AsProgram = "mytool"
	render.MoveToPath = true
	render.Insecure = true
	shared, err := h.execute(context.Background(), render)
	if err != nil {
		t.Fatal(err)
	}
	if releases, _ := f.counts(); releases != 2 {
		t.Fatalf("render-only variant refetched: %d", releases)
	}
	if shared.OS != "darwin" || shared.Arch != "arm64" || shared.AsProgram != "mytool" || !shared.MoveToPath || !shared.Insecure {
		t.Fatalf("per-request render options lost: %#v", shared.Query)
	}
	if len(shared.Assets) != 1 || shared.Assets[0].Name != "tool-linux-amd64.tar.gz" {
		t.Fatalf("render-only variant must reuse canonical assets: %#v", shared.Assets)
	}
}

func TestResolverSingleWaiterCancellation(t *testing.T) {
	f := newFakeGitHub(t)
	f.sums = true
	h := f.handler()
	r, _ := newTestResolver(t, h)
	q := latestQuery("")
	gate := make(chan struct{})
	f.setBlock(gate)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	errs := make(chan error, 2)
	var result QueryResult
	go func() { _, err := h.execute(ctx1, q); errs <- err }()
	go func() { result, _ = h.execute(ctx2, q); errs <- nil }()

	f.waitStarted(t)
	waitWaiters(t, r, keyFor(q), 2)
	cancel1()
	select {
	case err := <-errs:
		if err != context.Canceled {
			t.Fatalf("canceled waiter got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter never returned")
	}

	close(gate)
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("remaining waiter failed: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remaining waiter never returned")
	}
	if result.ResolvedRelease != "v1" || assetByName(result, "tool-linux-amd64.tar.gz").SHA256 == "" {
		t.Fatalf("unexpected shared result: %#v", result)
	}
	if releases, sums := f.counts(); releases != 1 || sums != 1 {
		t.Fatalf("single cancellation must not refetch, got %d/%d", releases, sums)
	}
}

func TestResolverAllWaitersCancelStopsIO(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	r, _ := newTestResolver(t, h)
	q := latestQuery("")
	gate := make(chan struct{})
	f.setBlock(gate)

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { _, err := h.execute(ctx, q); errs <- err }()
	f.waitStarted(t)
	waitWaiters(t, r, keyFor(q), 1)
	cancel()
	select {
	case err := <-errs:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request never returned")
	}
	select {
	case <-f.ioCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("background I/O was not canceled when all waiters left")
	}
	r.mu.Lock()
	_, present := r.flights[keyFor(q)]
	r.mu.Unlock()
	if present {
		t.Fatal("abandoned flight still registered")
	}

	if releases, _ := f.counts(); releases != 0 {
		t.Fatalf("canceled fetch must not complete a release response, got %d", releases)
	}

	close(gate)
	f.setBlock(nil)
	result, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResolvedRelease != "v1" {
		t.Fatalf("unexpected result after re-fetch: %s", result.ResolvedRelease)
	}
	if releases, _ := f.counts(); releases != 1 {
		t.Fatalf("expected one fresh fetch after full cancellation, got %d", releases)
	}
}

func TestResolverChecksumFailureNotCached(t *testing.T) {
	f := newFakeGitHub(t)
	f.sums = true
	f.sumsFail = true
	h := f.handler()
	r, _ := newTestResolver(t, h)
	q := latestQuery("")

	for i := 0; i < 2; i++ {
		if _, err := h.execute(context.Background(), q); err == nil || !strings.Contains(err.Error(), "checksums") {
			t.Fatalf("attempt %d: expected checksum error, got %v", i+1, err)
		}
	}
	if releases, sums := f.counts(); releases != 2 || sums != 2 {
		t.Fatalf("failures must not be cached, got %d/%d", releases, sums)
	}
	r.mu.Lock()
	_, cached := r.cache[keyFor(q)]
	r.mu.Unlock()
	if cached {
		t.Fatal("failure must not publish a cache entry")
	}

	f.mu.Lock()
	f.sumsFail = false
	f.mu.Unlock()
	result, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if assetByName(result, "tool-linux-amd64.tar.gz").SHA256 == "" {
		t.Fatal("successful refresh missing checksum")
	}
	before, _ := f.counts()
	if _, err := h.execute(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if after, _ := f.counts(); after != before {
		t.Fatalf("success was not cached: %d then %d", before, after)
	}
}

func TestResolverWithoutChecksumFileSucceeds(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	result, err := h.execute(context.Background(), latestQuery(""))
	if err != nil {
		t.Fatal(err)
	}
	if assetByName(result, "tool-linux-amd64.tar.gz").SHA256 != "" {
		t.Fatal("expected no checksum without a checksum asset")
	}
	if releases, sums := f.counts(); releases != 1 || sums != 0 {
		t.Fatalf("unexpected fetch counts %d/%d", releases, sums)
	}
}

func TestResolverStaleGenerationCannotOverwrite(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	r, _ := newTestResolver(t, h)
	q := latestQuery("")

	makeResult := func(tag string) QueryResult {
		return QueryResult{
			Query:           Query{User: q.User, Program: q.Program, Release: "latest"},
			ResolvedRelease: tag,
			Timestamp:       r.now(),
			Assets:          Assets{{OS: "linux", Arch: "amd64", Name: "tool-" + tag, URL: "https://example.invalid/" + tag}},
		}
	}
	gate1 := make(chan struct{})
	gate2 := make(chan struct{})
	firstFetch := make(chan struct{})
	secondFetch := make(chan struct{})
	var calls int32
	r.doFetch = func(ctx context.Context, q Query) (QueryResult, error) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			close(firstFetch)
			<-gate1
			return makeResult("v1"), nil
		default:
			close(secondFetch)
			<-gate2
			return makeResult("v2"), nil
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	err1 := make(chan error, 1)
	go func() { _, err := h.execute(ctx1, q); err1 <- err }()
	<-firstFetch
	cancel1()
	select {
	case err := <-err1:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first waiter never canceled")
	}

	err2 := make(chan error, 1)
	go func() {
		result, err := h.execute(context.Background(), q)
		if err == nil && result.ResolvedRelease != "v2" {
			err = fmt.Errorf("resolved %s", result.ResolvedRelease)
		}
		err2 <- err
	}()
	<-secondFetch
	close(gate2)
	if err := <-err2; err != nil {
		t.Fatal(err)
	}
	close(gate1)

	result, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResolvedRelease != "v2" {
		t.Fatalf("stale generation overwrote the cache with %s", result.ResolvedRelease)
	}
	if calls != 2 {
		t.Fatalf("expected 2 fetches, got %d", calls)
	}
}

func TestResolverReturnedAssetsAreCallerOwned(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	q := latestQuery("")

	first, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	first.Assets[0].Name = "mutated"
	first.Assets = append(first.Assets, Asset{Name: "extra"})

	second, err := h.execute(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Assets) != 2 || assetByName(second, "mutated").Name != "" {
		t.Fatalf("caller mutation leaked into cached result: %#v", second.Assets)
	}
}

func TestExecuteAndScriptEntriesShareResolver(t *testing.T) {
	f := newFakeGitHub(t)
	f.sums = true
	h := f.handler()
	r, _ := newTestResolver(t, h)
	q := latestQuery("")
	gate := make(chan struct{})
	f.setBlock(gate)

	var wg sync.WaitGroup
	wg.Add(2)
	jsonBody := []byte(nil)
	scriptBody := []byte(nil)
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/acme/tool@latest?type=json", nil))
		if w.Code != http.StatusOK {
			t.Errorf("json status %d: %s", w.Code, w.Body.String())
			return
		}
		jsonBody = append(jsonBody, w.Body.Bytes()...)
	}()
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/acme/tool@latest?type=script&os=freebsd", nil))
		if w.Code != http.StatusOK {
			t.Errorf("script status %d: %s", w.Code, w.Body.String())
			return
		}
		scriptBody = append(scriptBody, w.Body.Bytes()...)
	}()

	f.waitStarted(t)
	waitWaiters(t, r, keyFor(q), 2)
	close(gate)
	wg.Wait()

	if releases, sums := f.counts(); releases != 1 || sums != 1 {
		t.Fatalf("json and script entries must share one refresh, got %d/%d", releases, sums)
	}
	var result QueryResult
	if err := json.Unmarshal(jsonBody, &result); err != nil {
		t.Fatal(err)
	}
	if result.ResolvedRelease != "v1" || assetByName(result, "tool-linux-amd64.tar.gz").SHA256 == "" {
		t.Fatalf("unexpected json result: %#v", result)
	}
	script := string(scriptBody)
	for _, want := range []string{
		`OS="freebsd"`,
		"# v1",
		f.srv.URL + "/d/tool-linux-amd64.tar.gz",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
}

func TestResolverTTLPolicy(t *testing.T) {
	f := newFakeGitHub(t)
	h := f.handler()
	_, clk := newTestResolver(t, h)

	pinned := Query{User: "acme", Program: "tool", Release: "v9"}
	if _, err := h.execute(context.Background(), pinned); err != nil {
		t.Fatal(err)
	}
	clk.add(48 * time.Hour)
	if _, err := h.execute(context.Background(), pinned); err != nil {
		t.Fatal(err)
	}
	if releases, _ := f.counts(); releases != 1 {
		t.Fatalf("pinned release must not expire, got %d fetches", releases)
	}

	latest := latestQuery("")
	if _, err := h.execute(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	clk.add(cacheTTL + time.Minute)
	if _, err := h.execute(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	if releases, _ := f.counts(); releases != 3 {
		t.Fatalf("latest selector must refresh after TTL, got %d fetches", releases)
	}
}

func TestResolverConcurrentChurn(t *testing.T) {
	f := newFakeGitHub(t)
	f.sums = true
	h := f.handler()

	var wg sync.WaitGroup
	for _, selectAsset := range []string{"linux", "darwin", ""} {
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					if _, err := h.execute(context.Background(), latestQuery(selectAsset)); err != nil {
						t.Errorf("unexpected error: %s", err)
						return
					}
				}
			}()
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _ = h.execute(ctx, latestQuery(fmt.Sprintf("isolated-%d", i)))
		}(i)
	}
	wg.Wait()

	for _, selectAsset := range []string{"linux", "darwin", ""} {
		result, err := h.execute(context.Background(), latestQuery(selectAsset))
		if err != nil {
			t.Fatal(err)
		}
		if result.ResolvedRelease != "v1" {
			t.Fatalf("churn left inconsistent release %q", result.ResolvedRelease)
		}
	}
}
