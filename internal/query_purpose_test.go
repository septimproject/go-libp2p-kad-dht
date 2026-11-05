package internal

import (
	"context"
	"testing"
)

func TestQueryPurposeDefaultPreservesSpecificInitiator(t *testing.T) {
	t.Parallel()
	ctx := WithQueryPurpose(t.Context(), QuerySelfRefresh)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	got := QueryPurposeOf(DefaultQueryPurpose(ctx, QueryClosestLookup))
	if got != QuerySelfRefresh {
		t.Fatalf("nested lookup lost initiator: %v", got)
	}
}

func TestQueryPurposeDoesNotModifyParent(t *testing.T) {
	t.Parallel()
	parent := t.Context()
	_ = WithQueryPurpose(parent, QueryPeerAdmission)
	if got := QueryPurposeOf(parent); got != QueryUnspecified {
		t.Fatalf("parent mutated: %v", got)
	}
}
