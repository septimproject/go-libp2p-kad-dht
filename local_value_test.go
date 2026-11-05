package dht

import (
	"context"
	"errors"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/records"
)

var errInvalidLocalValue = errors.New("invalid local value")

type localValueValidator struct{}

func (localValueValidator) Validate(_ string, value []byte) error {
	if len(value) != 1 {
		return errInvalidLocalValue
	}
	return nil
}
func (localValueValidator) Select(_ string, values [][]byte) (int, error) {
	if values[1][0] > values[0][0] {
		return 1, nil
	}
	return 0, nil
}

func TestPutLocalValueUsesNativeValidationAndSelectionWithoutHost(t *testing.T) {
	t.Parallel()
	// No host or routing table: any attempted network operation would fail.
	d := &IpfsDHT{valueStore: records.NewValueStore(ds.NewMapDatastore(), localValueValidator{}, 0)}
	if err := d.PutLocalValue(t.Context(), "key", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := d.PutLocalValue(t.Context(), "key", []byte("1")); !errors.Is(err, records.ErrOldRecord) {
		t.Fatalf("old value bypassed native selection: %v", err)
	}
	if err := d.PutLocalValue(t.Context(), "key", nil); !errors.Is(err, errInvalidLocalValue) {
		t.Fatalf("bad value bypassed validation: %v", err)
	}
	r, err := d.valueStore.Get(t.Context(), "key")
	if err != nil || string(r.GetValue()) != "2" {
		t.Fatalf("stored=%v err=%v", r, err)
	}
}

func TestPutLocalValueCancellationPrecedesStorage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (&IpfsDHT{}).PutLocalValue(ctx, "key", []byte("2")); !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation before store access: %v", err)
	}
}
