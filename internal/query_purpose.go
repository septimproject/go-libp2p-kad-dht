package internal

import "context"

// QueryPurpose describes the local initiator, never a wire field or priority.
type QueryPurpose uint8

const (
	QueryUnspecified QueryPurpose = iota
	QueryPeerAdmission
	QueryPeerRecheck
	QuerySelfRefresh
	QueryBucketRefresh
	QueryValueLookup
	QueryValuePlacement
	QueryPeerLookup
	QueryClosestLookup
)

type queryPurposeKey struct{}

func QueryPurposeOf(ctx context.Context) QueryPurpose {
	purpose, _ := ctx.Value(queryPurposeKey{}).(QueryPurpose)
	return purpose
}

func WithQueryPurpose(ctx context.Context, purpose QueryPurpose) context.Context {
	return context.WithValue(ctx, queryPurposeKey{}, purpose)
}

func DefaultQueryPurpose(ctx context.Context, purpose QueryPurpose) context.Context {
	if QueryPurposeOf(ctx) != QueryUnspecified {
		return ctx
	}
	return WithQueryPurpose(ctx, purpose)
}
