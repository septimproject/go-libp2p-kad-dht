package dht

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	pstore "github.com/libp2p/go-libp2p/p2p/host/peerstore"
)

type followupTestHost struct{ host.Host }

func (followupTestHost) Network() network.Network { return followupTestNetwork{} }

type followupTestNetwork struct{ network.Network }

func (followupTestNetwork) Connectedness(peer.ID) network.Connectedness { return network.Connected }

func TestFollowupRespectsLookupConcurrency(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const concurrency = 2
		rt, err := kb.NewRoutingTable(8, kb.ConvertPeerID("local"), time.Minute, pstore.NewMetrics(), time.Hour, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer rt.Close()
		for i := range 8 {
			if added, err := rt.TryAddPeer(peer.ID(fmt.Sprintf("peer-%d", i)), true, false); err != nil || !added {
				t.Fatalf("routing fixture: added=%v err=%v", added, err)
			}
		}
		d := &IpfsDHT{ctx: t.Context(), host: followupTestHost{}, self: "local", routingTable: rt, bucketSize: 8, alpha: concurrency, beta: 1, addPeerToRTChan: make(chan peer.ID, 32)}
		var active, peak, calls atomic.Int32
		fn := func(context.Context, peer.ID) ([]*peer.AddrInfo, error) {
			calls.Add(1)
			current := active.Add(1)
			for previous := peak.Load(); current > previous; previous = peak.Load() {
				if peak.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			return nil, nil
		}

		_, err = d.runLookupWithFollowup(t.Context(), "target", fn, func(*qpeerset.QueryPeerset) bool { return false })

		if err != nil {
			t.Fatal(err)
		}
		if calls.Load() < 8 || peak.Load() > concurrency {
			t.Fatalf("follow-up fanout escaped lookup concurrency: calls=%d peak=%d", calls.Load(), peak.Load())
		}
	})
}
