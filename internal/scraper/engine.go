package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TargetPlaceholder is replaced with the (URL-escaped) target ID in SourceURL.
const TargetPlaceholder = "{target}"

// ErrNoSource is returned by FetchTarget when no SourceURL is configured.
var ErrNoSource = errors.New("scraper: no SOURCE_URL configured")

// Chapter is a single tracked release for a target.
type Chapter struct {
	Target string
	Number string
	URL    string
	Title  string
	Seen   time.Time
}

// Engine runs manga/manhwa tracking jobs for the targets assigned to this
// node, polling on an interval and dispatching new chapters to a webhook.
//
// One poll of one target is four separate steps, kept apart on purpose:
//
//	fetch   (network)  -> raw bytes          FetchTarget
//	parse   (pure)     -> []Chapter          ParseChapters
//	detect  (pure)     -> new chapters       DetectNew
//	notify  (network)  -> webhook, then mark MarkSeen only after an ack
type Engine struct {
	mu      sync.Mutex
	targets map[string]struct{} // target IDs currently owned by this node
	// highWater is the highest chapter number already announced per target. A
	// target with no entry has not been baselined yet.
	highWater map[string]float64

	WebhookURL string
	// SourceURL is the per-target chapter-list endpoint; "{target}" is replaced
	// with the target ID. The endpoint must return a JSON array of
	// {"number": "12" | 12, "title": "...", "url": "..."} (see ParseChapters).
	SourceURL  string
	HTTPClient *http.Client
}

// NewEngine constructs an Engine that will post new-chapter notifications
// to webhookURL.
func NewEngine(webhookURL string) *Engine {
	return &Engine{
		targets:    make(map[string]struct{}),
		highWater:  make(map[string]float64),
		WebhookURL: webhookURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// SetTargets replaces the set of target IDs this engine is responsible for,
// e.g. after the hash ring reassigns work. State for targets no longer owned is
// dropped: this node must not announce anything for them any more.
func (e *Engine) SetTargets(targets []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.targets = make(map[string]struct{}, len(targets))
	for _, t := range targets {
		e.targets[t] = struct{}{}
	}
	for t := range e.highWater {
		if _, ok := e.targets[t]; !ok {
			delete(e.highWater, t)
		}
	}
}

func (e *Engine) owns(target string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.targets[target]
	return ok
}

// Run polls every target on interval until ctx is cancelled. It polls once
// immediately so a freshly started worker doesn't sit idle for a full interval.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	e.pollAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.pollAll(ctx)
		}
	}
}

func (e *Engine) pollAll(ctx context.Context) {
	e.mu.Lock()
	targets := make([]string, 0, len(e.targets))
	for t := range e.targets {
		targets = append(targets, t)
	}
	e.mu.Unlock()
	sort.Strings(targets)

	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		e.pollTarget(ctx, target)
	}
}

// pollTarget runs fetch -> parse -> detect -> notify for one target. Failures
// are logged and leave the high-water mark untouched, so the next poll simply
// tries again.
func (e *Engine) pollTarget(ctx context.Context, target string) {
	body, err := e.FetchTarget(ctx, target)
	if err != nil {
		log.Printf("scraper: fetch %s: %v", target, err)
		return
	}
	chapters, err := e.ParseChapters(target, body)
	if err != nil {
		log.Printf("scraper: parse %s: %v", target, err)
		return
	}

	fresh, baselined := e.DetectNew(target, chapters)
	if baselined {
		log.Printf("scraper: %s baselined at %d existing chapter(s); not announcing them", target, len(chapters))
		return
	}

	// Oldest first, so a failure part-way leaves the mark at the last chapter
	// that really was delivered and the rest retry in order.
	for _, ch := range fresh {
		// Ownership may have moved while we were on the network. Announcing for
		// a target we no longer own would duplicate the new owner's work.
		if !e.owns(target) {
			return
		}
		if err := e.PostToWebhook(ctx, ch); err != nil {
			log.Printf("scraper: webhook for %s ch.%s failed, will retry: %v", target, ch.Number, err)
			return
		}
		e.MarkSeen(ch)
		log.Printf("scraper: announced %s ch.%s", target, ch.Number)
	}
}

// FetchTarget performs the network request for one target and returns the raw
// response body. It does no interpretation; that is ParseChapters' job.
func (e *Engine) FetchTarget(ctx context.Context, target string) ([]byte, error) {
	if e.SourceURL == "" {
		return nil, ErrNoSource
	}
	u := strings.ReplaceAll(e.SourceURL, TargetPlaceholder, url.PathEscape(target))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("source returned %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}

type sourceChapter struct {
	Number json.RawMessage `json:"number"` // accepts 12, 12.5 and "12"
	Title  string          `json:"title"`
	URL    string          `json:"url"`
}

// ParseChapters extracts Chapter entries from a raw source response body. It
// is pure (no network), so it is unit tested directly. Entries whose number is
// not numeric are skipped rather than failing the whole page.
func (e *Engine) ParseChapters(target string, body []byte) ([]Chapter, error) {
	var raw []sourceChapter
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode chapter list: %w", err)
	}
	now := time.Now()
	out := make([]Chapter, 0, len(raw))
	for _, r := range raw {
		num := strings.Trim(string(r.Number), `"`)
		if _, err := strconv.ParseFloat(num, 64); err != nil {
			continue
		}
		out = append(out, Chapter{Target: target, Number: num, URL: r.URL, Title: r.Title, Seen: now})
	}
	return out, nil
}

// DetectNew returns the chapters of target newer than its high-water mark,
// oldest first. It never changes state except for one case: the first time a
// target is seen, its mark is set to the current newest chapter and nothing is
// returned (baselined == true), so adding a series with 200 existing chapters
// doesn't flood the channel.
//
// Numbers are compared as numbers: "10" is newer than "9", and "10.5" newer
// than "10". A string compare gets both wrong.
func (e *Engine) DetectNew(target string, chapters []Chapter) (fresh []Chapter, baselined bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	type numbered struct {
		ch Chapter
		n  float64
	}
	var all []numbered
	for _, ch := range chapters {
		n, err := strconv.ParseFloat(ch.Number, 64)
		if err != nil {
			continue
		}
		all = append(all, numbered{ch, n})
	}
	if len(all) == 0 {
		return nil, false
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n < all[j].n })

	mark, ok := e.highWater[target]
	if !ok {
		e.highWater[target] = all[len(all)-1].n
		return nil, true
	}
	for _, c := range all {
		if c.n > mark {
			fresh = append(fresh, c.ch)
		}
	}
	return fresh, false
}

// MarkSeen advances the target's high-water mark to ch. Call it only after the
// webhook acknowledged the chapter: until then the chapter is still "new".
func (e *Engine) MarkSeen(ch Chapter) {
	n, err := strconv.ParseFloat(ch.Number, 64)
	if err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur, ok := e.highWater[ch.Target]; !ok || n > cur {
		e.highWater[ch.Target] = n
	}
}

// PostToWebhook delivers a new-chapter notification to WebhookURL (Discord
// webhook format). Any 2xx is an ack; everything else is an error.
func (e *Engine) PostToWebhook(ctx context.Context, ch Chapter) error {
	if e.WebhookURL == "" {
		return errors.New("no webhook URL configured")
	}
	title := fmt.Sprintf("%s — Chapter %s", ch.Target, ch.Number)
	if ch.Title != "" {
		title += ": " + ch.Title
	}
	payload, err := json.Marshal(map[string]any{
		"embeds": []map[string]any{{
			"title":     title,
			"url":       ch.URL,
			"timestamp": ch.Seen.UTC().Format(time.RFC3339),
		}},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}
