package worker

import (
	"context"
	"io"
	"log/slog"
)

// NewLogger returns a JSON logger that stamps every line with the node ID and
// the current Raft term. term is called per record, so lines logged across an
// election show the term change. Call sites add target / epoch attributes
// where they have them.
func NewLogger(w io.Writer, node string, term func() uint64) *slog.Logger {
	h := termHandler{Handler: slog.NewJSONHandler(w, nil), term: term}
	return slog.New(h).With("node", node)
}

type termHandler struct {
	slog.Handler
	term func() uint64
}

func (h termHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(slog.Uint64("term", h.term()))
	return h.Handler.Handle(ctx, r)
}

func (h termHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return termHandler{Handler: h.Handler.WithAttrs(attrs), term: h.term}
}

func (h termHandler) WithGroup(name string) slog.Handler {
	return termHandler{Handler: h.Handler.WithGroup(name), term: h.term}
}
