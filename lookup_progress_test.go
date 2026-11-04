package dht

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	pstore "github.com/libp2p/go-libp2p/p2p/host/peerstore"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type progressTestSender struct {
	pb.MessageSender
	request func(context.Context, peer.ID) (*pb.Message, error)
}

func (s progressTestSender) SendRequest(ctx context.Context, id peer.ID, _ *pb.Message) (*pb.Message, error) {
	return s.request(ctx, id)
}
func progressTestDHT(t *testing.T, request func(context.Context, peer.ID) (*pb.Message, error)) *IpfsDHT {
	t.Helper()
	rt, err := kb.NewRoutingTable(2, kb.ConvertPeerID("local"), time.Minute, pstore.NewMetrics(), time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	for _, id := range []peer.ID{"fast", "slow"} {
		if added, err := rt.TryAddPeer(id, true, false); err != nil || !added {
			t.Fatalf("routing fixture: added=%v err=%v", added, err)
		}
	}
	messenger, err := pb.NewProtocolMessenger(progressTestSender{request: request})
	if err != nil {
		t.Fatal(err)
	}
	d := &IpfsDHT{ctx: t.Context(), host: followupTestHost{}, self: "local", routingTable: rt, bucketSize: 2, alpha: 2, beta: 2, addPeerToRTChan: make(chan peer.ID, 16), protoMessenger: messenger}
	d.nsEstimator = netsize.NewEstimator(d.self, rt, 2)
	return d
}
func TestClosestPeersProgressPrecedesLookupCompletion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		slowStarted, fastReported := make(chan struct{}), make(chan struct{})
		d := progressTestDHT(t, func(_ context.Context, id peer.ID) (*pb.Message, error) {
			if id == "slow" {
				close(slowStarted)
				<-fastReported
			} else {
				<-slowStarted
			}
			return &pb.Message{}, nil
		})
		for i := range netsize.MinMeasurementsThreshold {
			key := fmt.Sprintf("sample-%d", i)
			ids := kb.SortClosestPeers([]peer.ID{"fast", "slow"}, kb.ConvertKey(key))
			if err := d.nsEstimator.Track(key, ids); err != nil {
				t.Fatal(err)
			}
		}
		if size, err := d.NetworkSize(); err != nil || size <= 0 {
			t.Fatalf("network-size fixture: %d %v", size, err)
		}
		var reports atomic.Int32
		ids, err := d.GetClosestPeersWithProgress(t.Context(), "fast", func(id peer.ID) {
			if id == "fast" {
				reports.Add(1)
				close(fastReported)
			}
		})
		if err != nil || reports.Load() != 1 || len(ids) != 2 || !slices.Contains(ids, peer.ID("fast")) || !slices.Contains(ids, peer.ID("slow")) {
			t.Fatalf("progress changed final lookup: peers=%v reports=%d err=%v", ids, reports.Load(), err)
		}
	})
}
func TestClosestPeersProgressColdEstimateKeepsNativeResult(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		d := progressTestDHT(t, func(context.Context, peer.ID) (*pb.Message, error) { return &pb.Message{}, nil })
		var reports atomic.Int32
		ids, err := d.GetClosestPeersWithProgress(t.Context(), "fast", func(peer.ID) { reports.Add(1) })
		if err != nil || len(ids) != 2 || reports.Load() != 0 {
			t.Fatalf("cold estimate changed native lookup: peers=%v reports=%d err=%v", ids, reports.Load(), err)
		}
	})
}
