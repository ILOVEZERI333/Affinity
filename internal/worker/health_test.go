package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/affinity/distriscrape/internal/metrics"
)

func TestLivenessHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path status = %d, want 404", rec.Code)
	}
}

func TestProberUnhealthyAfterFourFailures(t *testing.T) {
	p := NewProber(func(context.Context) error { return errors.New("down") }, metrics.New("c", "n"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx, time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for p.Healthy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.Healthy() {
		t.Fatal("still healthy after repeated failures")
	}
}

func TestReadyzFollowsReadiness(t *testing.T) {
	ready := false
	h := HealthHandler(func() bool { return ready }, http.NotFoundHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("not ready = %d, want 503", rec.Code)
	}
	ready = true
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready = %d, want 200", rec.Code)
	}
}

func TestProberRecoversAfterSuccess(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	p := NewProber(func(context.Context) error {
		if failing.Load() {
			return errors.New("down")
		}
		return nil
	}, metrics.New("c", "n"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx, time.Millisecond)

	wait := func(want bool, why string) {
		deadline := time.Now().Add(2 * time.Second)
		for p.Healthy() != want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if p.Healthy() != want {
			t.Fatal(why)
		}
	}
	wait(false, "never became unhealthy")
	failing.Store(false)
	wait(true, "did not recover after a passing check")
}
