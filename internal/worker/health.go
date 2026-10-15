package worker

import "net/http"

// LivenessHandler serves the Kubernetes liveness probe (mechanism #1 in the
// Phase 9 order). It deliberately checks only that the process can answer:
// it must not depend on Raft or peers, or a cluster-wide hiccup would make
// Kubernetes restart healthy pods.
func LivenessHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
