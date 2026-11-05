package dht

import (
	"context"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
)

// QueryPurpose is bounded local diagnostic metadata. It grants no resources,
// changes no lookup behavior and is never decoded from a remote message.
type QueryPurpose = internal.QueryPurpose

const (
	QueryUnspecified    = internal.QueryUnspecified
	QueryPeerAdmission  = internal.QueryPeerAdmission
	QueryPeerRecheck    = internal.QueryPeerRecheck
	QuerySelfRefresh    = internal.QuerySelfRefresh
	QueryBucketRefresh  = internal.QueryBucketRefresh
	QueryValueLookup    = internal.QueryValueLookup
	QueryValuePlacement = internal.QueryValuePlacement
	QueryPeerLookup     = internal.QueryPeerLookup
	QueryClosestLookup  = internal.QueryClosestLookup
)

// QueryPurposeOf reports the local cause inherited by a request context.
func QueryPurposeOf(ctx context.Context) QueryPurpose { return internal.QueryPurposeOf(ctx) }

// WithQueryPurpose labels an adapter-owned lookup for diagnostics only. Native
// maintenance overrides it at its own entry points; admission ignores it.
func WithQueryPurpose(ctx context.Context, purpose QueryPurpose) context.Context {
	return internal.WithQueryPurpose(ctx, purpose)
}
