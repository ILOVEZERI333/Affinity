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
	"strings"
	"sync"
	"time"
)

const (
	ackAttempts   = 3
	ackRetryDelay = 200 * time.Millisecond
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
//	detect  (outbox)   -> new chapters       DetectNew
//	notify  (network)  -> Enqueue, POST webhook, Ack (see Outbox)
//
// What has been announced lives in the Outbox, not in the engine, so it
// survives restarts and follows a target when the ring moves it.
type Engine struct {
	mu      sync.Mutex
	targets map[string]struct{} // target IDs currently owned by this node
	retry   map[string]retryState

	// Outbox records pending and delivered notifications. NewEngine installs an
	// in-process one; the worker swaps in the Raft-replicated one.
	Outbox Outbox

	// BackoffBase and BackoffMax bound the per-target retry delay after a failed
	// poll: BackoffBase, doubling per consecutive failure, capped at BackoffMax.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	now         func() time.Time

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
		targets:     make(map[string]struct{}),
		retry:       make(map[string]retryState),
		Outbox:      NewLocalOutbox(),
		BackoffBase: 5 * time.Second,
		BackoffMax:  10 * time.Minute,
		now:         time.Now,
		WebhookURL:  webhookURL,
		HTTPClient:  &http.Client{Timeout: 10 * time.Second},
	}
}

// SetTargets replaces the set of target IDs this engine is responsible for,
// e.g. after the hash ring reassigns work. Retry state for targets no longer
// owned is dropped: this node must not announce anything for them any more.
func (e *Engine) SetTargets(targets []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.targets = make(map[string]struct{}, len(targets))
	for _, t := range targets {
		e.targets[t] = struct{}{}
	}
	for t := range e.retry {
		if _, ok := e.targets[t]; !ok {
			delete(e.retry, t)
		}
	}
}

// outboxError marks a failure of the delivery store rather than of the source
// or the webhook; see recordResult.
type outboxError struct{ err error }

func (e *outboxError) Error() string { return "outbox: " + e.err.Error() }
func (e *outboxError) Unwrap() error { return e.err }

// retryState tracks consecutive failures of one target.
type retryState struct {
	fails int
	next  time.Time
}

// backoffDelay is BackoffBase doubled per failure beyond the first, capped.
func (e *Engine) backoffDelay(fails int) time.Duration {
	d := e.BackoffBase
	for i := 1; i < fails && d < e.BackoffMax; i++ {
		d *= 2
	}
	return min(d, e.BackoffMax)
}

// backingOff reports whether target is still inside its retry delay.
func (e *Engine) backingOff(target string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now().Before(e.retry[target].next)
}

// recordResult clears a target's backoff on success and extends it on failure.
func (e *Engine) recordResult(target string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.targets[target]; !ok {
		return
	}
	if err == nil {
		delete(e.retry, target)
		return
	}
	// Outbox failures (no leader mid-election, fenced by an epoch bump) clear
	// within seconds and hit every target at once. Backing each off for minutes
	// would delay announcements long after the cluster recovered; the next tick
	// retries instead. Shutdown is not a failure at all.
	var oe *outboxError
	if errors.As(err, &oe) || errors.Is(err, context.Canceled) {
		return
	}
	st := e.retry[target]
	st.fails++
	delay := e.backoffDelay(st.fails)
	st.next = e.now().Add(delay)
	e.retry[target] = st
	log.Printf("scraper: %s: %v (failure %d, next try in %s)", target, err, st.fails, delay)
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
		if e.backingOff(target) {
			continue
		}
		e.recordResult(target, e.pollTarget(ctx, target))
	}
}

// pollTarget runs fetch -> parse -> detect -> notify for one target. A returned
// error leaves the chapter undelivered and puts the target into backoff; the
// next attempt starts from the outbox, so nothing is lost or skipped.
//
// Per chapter, oldest first:
//
//	Enqueue  record it pending (or learn it is already delivered)
//	POST     webhook
//	Ack      mark delivered, advancing the high-water mark
//
// A failure stops the target's remaining chapters so order is preserved.
func (e *Engine) pollTarget(ctx context.Context, target string) error {
	body, err := e.FetchTarget(ctx, target)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	chapters, err := e.ParseChapters(target, body)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	fresh, baselined, err := e.DetectNew(ctx, target, chapters)
	if err != nil {
		return &outboxError{fmt.Errorf("detect: %w", err)}
	}
	if baselined {
		log.Printf("scraper: %s baselined at %d existing chapter(s); not announcing them", target, len(chapters))
		return nil
	}

	for _, ch := range fresh {
		// Ownership may move while we are on the network. Announcing for a
		// target we no longer own would duplicate the new owner's work.
		if !e.owns(target) {
			return nil
		}
		state, err := e.Outbox.Enqueue(ctx, ch)
		if err != nil {
			return &outboxError{fmt.Errorf("enqueue ch.%s: %w", ch.Number, err)}
		}
		if state == StateDelivered {
			// Our view of the mark lagged: it was already delivered.
			continue
		}
		if !e.owns(target) {
			return nil
		}
		if err := e.PostToWebhook(ctx, ch); err != nil {
			return fmt.Errorf("webhook ch.%s (stays pending): %w", ch.Number, err)
		}
		if err := e.ack(ctx, ch); err != nil {
			return &outboxError{fmt.Errorf("ack ch.%s: %w", ch.Number, err)}
		}
		log.Printf("scraper: announced %s ch.%s", target, ch.Number)
	}
	return nil
}

// ack records a delivery, retrying briefly: the webhook already fired, so a
// failed ack is the one window in which the chapter would be announced again.
func (e *Engine) ack(ctx context.Context, ch Chapter) (err error) {
	for attempt := 1; attempt <= ackAttempts; attempt++ {
		if err = e.Outbox.Ack(ctx, ch); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * ackRetryDelay):
		}
	}
	return err
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
		if _, err := ParseChapterNumber(num); err != nil {
			continue
		}
		out = append(out, Chapter{Target: target, Number: num, URL: r.URL, Title: r.Title, Seen: now})
	}
	return out, nil
}

// DetectNew returns the chapters of target newer than its high-water mark,
// oldest first. The only state it changes is the first-sight baseline: the
// first time a target is seen, its mark is set to the current newest chapter
// and nothing is returned (baselined == true), so adding a series with 200
// existing chapters doesn't flood the channel.
//
// The mark comes from the Outbox, so a target that moved here keeps the history
// of the node that owned it before. The mark can lag the replicated truth; that
// only means a delivered chapter is returned again and Enqueue reports it
// delivered.
//
// Numbers are compared as numbers: "10" is newer than "9", and "10.5" newer
// than "10". A string compare gets both wrong.
func (e *Engine) DetectNew(ctx context.Context, target string, chapters []Chapter) (fresh []Chapter, baselined bool, err error) {
	type numbered struct {
		ch Chapter
		n  float64
	}
	var all []numbered
	for _, ch := range chapters {
		n, err := ParseChapterNumber(ch.Number)
		if err != nil {
			continue
		}
		all = append(all, numbered{ch, n})
	}
	if len(all) == 0 {
		return nil, false, nil
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n < all[j].n })

	mark, ok := e.Outbox.HighWater(target)
	if !ok {
		if err := e.Outbox.Baseline(ctx, target, all[len(all)-1].n); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	for _, c := range all {
		if c.n > mark {
			fresh = append(fresh, c.ch)
		}
	}
	return fresh, false, nil
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
	// Discord ignores this; a receiver that dedupes can use it to make a repeat
	// POST (after a timeout we could not resolve) a no-op.
	req.Header.Set("Idempotency-Key", ch.Target+":"+ch.Number)
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
