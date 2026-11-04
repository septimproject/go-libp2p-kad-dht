package dht

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestValueQuorumStopsAtRequestedResponseCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		quorum int
	}{
		{"one response", 1},
		{"two responses", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				values := make(chan recvdVal, tc.quorum)
				for i := range tc.quorum {
					values <- recvdVal{Val: []byte("value"), From: peer.ID([]byte{byte(i)})}
				}
				// Keep the channel open: later replicas need not respond once
				// the requested number of validated values has arrived.
				stop := make(chan struct{})
				out := make(chan []byte, 1)
				d := &IpfsDHT{}

				best, _, stopped := d.searchValueQuorum(ctx, "/v/key", values, stop, out, tc.quorum)

				if ctx.Err() != nil || !stopped || string(best) != "value" {
					t.Fatalf("quorum waited for an extra response: value=%q stopped=%v err=%v", best, stopped, ctx.Err())
				}
				select {
				case <-stop:
				default:
					t.Fatal("satisfied quorum did not stop the remaining lookup")
				}
			})
		})
	}
}
