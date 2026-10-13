package handler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
)

type resolveKey struct {
	User    string `json:"user"`
	Program string `json:"program"`
	Release string `json:"release"`
	Select  string `json:"select"`
	Search  bool   `json:"search"`
}

func keyFor(q Query) string {
	release := q.Release
	if release == "" {
		release = "latest"
	}
	hw := sha256.New()
	jw := json.NewEncoder(hw)
	if err := jw.Encode(resolveKey{
		User:    q.User,
		Program: q.Program,
		Release: release,
		Select:  q.Select,
		Search:  q.Search,
	}); err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(hw.Sum(nil))
}

type cacheEntry struct {
	result QueryResult
	at     time.Time
	ttl    time.Duration
}

func (e cacheEntry) freshAt(now time.Time) bool {
	return e.ttl == 0 || now.Sub(e.at) < e.ttl
}

func ttlForSelector(selector string) time.Duration {
	if selector == "" || selector == "latest" {
		return cacheTTL
	}
	return 0
}

type flight struct {
	key     string
	q       Query
	gen     uint64
	started time.Time
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	waiters int
	result  QueryResult
	err     error
}

type resolver struct {
	h       *Handler
	mu      sync.Mutex
	cache   map[string]cacheEntry
	flights map[string]*flight
	gens    map[string]uint64
	now     func() time.Time
	doFetch func(context.Context, Query) (QueryResult, error)
}

func newResolver(h *Handler) *resolver {
	r := &resolver{
		h:       h,
		cache:   map[string]cacheEntry{},
		flights: map[string]*flight{},
		gens:    map[string]uint64{},
		now:     time.Now,
	}
	r.doFetch = r.fetchResult
	return r
}

func (r *resolver) bumpGen(key string) uint64 {
	r.gens[key]++
	return r.gens[key]
}

func cloneResult(result QueryResult) QueryResult {
	result.Assets = append(Assets(nil), result.Assets...)
	return result
}

func canonicalResult(result QueryResult) QueryResult {
	result.Query.AsProgram = ""
	result.Query.MoveToPath = false
	result.Query.Insecure = false
	result.Query.OS = ""
	result.Query.Arch = ""
	return result
}

func withRequest(q Query, result QueryResult) QueryResult {
	result.Query.AsProgram = q.AsProgram
	result.Query.MoveToPath = q.MoveToPath
	result.Query.Insecure = q.Insecure
	result.Query.OS = q.OS
	result.Query.Arch = q.Arch
	return result
}

func (r *resolver) resolve(ctx context.Context, q Query) (QueryResult, error) {
	key := keyFor(q)
	r.mu.Lock()
	if entry, ok := r.cache[key]; ok && entry.freshAt(r.now()) {
		r.mu.Unlock()
		return withRequest(q, cloneResult(entry.result)), nil
	}
	f, ok := r.flights[key]
	if !ok {
		fctx, cancel := context.WithCancel(context.Background())
		f = &flight{
			key:     key,
			q:       q,
			gen:     r.bumpGen(key),
			started: r.now(),
			ctx:     fctx,
			cancel:  cancel,
			done:    make(chan struct{}),
			waiters: 1,
		}
		r.flights[key] = f
		r.mu.Unlock()
		go r.run(f)
	} else {
		f.waiters++
		r.mu.Unlock()
	}
	result, err := r.await(ctx, f)
	if err != nil {
		return QueryResult{}, err
	}
	return withRequest(q, result), nil
}

func (r *resolver) await(ctx context.Context, f *flight) (QueryResult, error) {
	select {
	case <-f.done:
		return cloneResult(f.result), f.err
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-f.done:
		return cloneResult(f.result), f.err
	default:
	}
	f.waiters--
	if f.waiters == 0 && r.flights[f.key] == f {
		delete(r.flights, f.key)
		r.bumpGen(f.key)
		f.cancel()
	}
	return QueryResult{}, ctx.Err()
}

func (r *resolver) run(f *flight) {
	result, err := r.doFetch(f.ctx, f.q)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.flights[f.key] != f || r.gens[f.key] != f.gen {
		return
	}
	delete(r.flights, f.key)
	if err == nil {
		result = canonicalResult(result)
		r.publish(f, result)
	}
	f.result = cloneResult(result)
	f.err = err
	close(f.done)
}

func (r *resolver) publish(f *flight, result QueryResult) {
	keys := []string{f.key}
	if resolved := keyFor(result.Query); resolved != f.key {
		keys = append(keys, resolved)
	}
	at := r.now()
	for _, key := range keys {
		if other, ok := r.flights[key]; ok && other != f {
			continue
		}
		if current, ok := r.cache[key]; ok && current.at.After(f.started) {
			continue
		}
		selector := result.Query.Release
		r.cache[key] = cacheEntry{result: cloneResult(result), at: at, ttl: ttlForSelector(selector)}
	}
}

func (r *resolver) fetchResult(ctx context.Context, q Query) (QueryResult, error) {
	h := r.h
	release, assets, err := h.getAssetsNoCache(ctx, q)
	if err == nil {
		q.Search = false
	} else if errors.Is(err, errNotFound) && q.Search {
		user, program, gerr := imFeelingLuck(ctx, q.Program)
		if gerr != nil {
			log.Printf("web search failed: %s", gerr)
		} else {
			log.Printf("web search found: %s/%s", user, program)
			if program != q.Program {
				log.Printf("program mismatch: got %s: expected %s", q.Program, program)
			}
			q.Program = program
			q.User = user
			release, assets, err = h.getAssetsNoCache(ctx, q)
		}
	}
	if err != nil {
		return QueryResult{}, err
	}
	if q.Release == "" && release != "" {
		log.Printf("detected release: %s", release)
		q.Release = release
	}
	return QueryResult{
		Timestamp:       r.now(),
		Query:           q,
		ResolvedRelease: release,
		Assets:          assets,
		M1Asset:         assets.HasM1(),
	}, nil
}

func (h *Handler) resolver() *resolver {
	h.resOnce.Do(func() {
		h.res = newResolver(h)
	})
	return h.res
}

func (h *Handler) execute(ctx context.Context, q Query) (QueryResult, error) {
	return h.resolver().resolve(ctx, q)
}
