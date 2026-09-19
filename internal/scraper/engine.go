package scraper

import (
	"context"
	"sync"
	"time"
)

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
type Engine struct {
	mu              sync.Mutex
	targets         map[string]struct{} // target IDs currently owned by this node
	lastSeenChapter map[string]string   // target ID -> last chapter number posted

	WebhookURL string
}

// NewEngine constructs an Engine that will post new-chapter notifications
// to webhookURL.
func NewEngine(webhookURL string) *Engine {
	return &Engine{
		targets:         make(map[string]struct{}),
		lastSeenChapter: make(map[string]string),
		WebhookURL:      webhookURL,
	}
}

// SetTargets replaces the set of target IDs this engine is responsible for,
// e.g. after the hash ring reassigns work.
func (e *Engine) SetTargets(targets []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.targets = make(map[string]struct{}, len(targets))
	for _, t := range targets {
		e.targets[t] = struct{}{}
	}
}

// Run polls every target on interval until ctx is cancelled.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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

	for _, target := range targets {
		chapters, err := e.FetchTarget(ctx, target)
		if err != nil {
			// TODO: structured logging + backoff/retry policy per target.
			continue
		}
		for _, ch := range chapters {
			if e.DetectNewChapter(ch) {
				// TODO: decide error handling/retry for a failed webhook post.
				_ = e.PostToWebhook(ctx, ch)
			}
		}
	}
}

// FetchTarget retrieves the current chapter list for a single target
// (source-specific: HTTP GET + HTML/JSON parsing).
func (e *Engine) FetchTarget(ctx context.Context, target string) ([]Chapter, error) {
	// TODO: fetch the target's source page/API and parse it into Chapters.
	return nil, nil
}

// ParseChapters extracts Chapter entries from a raw source page/response
// body. Split out from FetchTarget so parsing logic can be unit tested
// without a network call.
func (e *Engine) ParseChapters(target string, body []byte) ([]Chapter, error) {
	// TODO: HTML/JSON parsing specific to the tracked source.
	return nil, nil
}

// DetectNewChapter reports whether ch is newer than the last chapter this
// engine has recorded for ch.Target, and updates the recorded high-water
// mark if so.
func (e *Engine) DetectNewChapter(ch Chapter) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	// TODO: compare ch.Number against e.lastSeenChapter[ch.Target] (numeric,
	// not lexicographic, comparison) and update the map on a new chapter.
	return false
}

// PostToWebhook delivers a new-chapter notification to WebhookURL (Discord
// webhook format).
func (e *Engine) PostToWebhook(ctx context.Context, ch Chapter) error {
	// TODO: build the Discord embed payload and POST it to e.WebhookURL.
	return nil
}
