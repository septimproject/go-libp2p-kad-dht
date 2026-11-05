package dht

import (
	"context"
	"slices"

	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/routing"
)

// PutLocalValue validates and selects a record through the same serialized value
// store used by PUT_VALUE. It performs no lookup, network request or replication.
// This allows a bounded anti-entropy adapter to repair local storage without
// depending on datastore key encoding or racing native record selection.
func (dht *IpfsDHT) PutLocalValue(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dht.valueStore == nil {
		return routing.ErrNotSupported
	}
	return dht.valueStore.Put(ctx, key, record.MakePutRecord(key, slices.Clone(value)))
}
