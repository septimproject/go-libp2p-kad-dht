package dht

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	pstore "github.com/libp2p/go-libp2p/p2p/host/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

func lookupCheckFixture(t *testing.T, request func(context.Context, peer.ID) (*pb.Message, error)) *IpfsDHT {
	t.Helper()
	store, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	const name protocol.ID = "/test/kad/1"
	for _, id := range []peer.ID{"same", "other", "third"} {
		if err := store.AddProtocols(id, name); err != nil {
			t.Fatal(err)
		}
	}
	rt, err := kb.NewRoutingTable(8, kb.ConvertPeerID("local"), time.Minute, pstore.NewMetrics(), time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	messenger, err := pb.NewProtocolMessenger(progressTestSender{request: request})
	if err != nil {
		t.Fatal(err)
	}
	return &IpfsDHT{ctx: t.Context(), peerstore: store, protocols: []protocol.ID{name}, routingTable: rt, bucketSize: 8, lookupCheckCapacity: 2, lookupCheckTimeout: time.Second, protoMessenger: messenger, addPeerToRTChan: make(chan peer.ID, 4)}
}

func TestPeerFoundCoalescesSameIdentityWithoutBlockingAnother(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		calls := make(chan peer.ID, 8)
		d := lookupCheckFixture(t, func(_ context.Context, id peer.ID) (*pb.Message, error) {
			calls <- id
			<-release
			return &pb.Message{}, nil
		})
		d.peerFound("same")
		synctest.Wait()
		for range 32 {
			d.peerFound("same")
		}
		d.peerFound("other")
		synctest.Wait()
		before := d.LookupCheckStats()
		close(release)
		synctest.Wait()
		after := d.LookupCheckStats()
		if len(calls) != 2 || before.Active != 2 || before.Started != 2 || before.Coalesced != 32 || after.Active != 0 {
			t.Fatalf("duplicate admission consumed RPCs or retained work: calls=%d before=%+v after=%+v", len(calls), before, after)
		}
	})
}

func TestFailedPeerAdmissionCanBeCheckedAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		d := lookupCheckFixture(t, func(context.Context, peer.ID) (*pb.Message, error) {
			attempts++
			return nil, errors.New("fixture unavailable")
		})
		d.peerFound("same")
		synctest.Wait()
		d.peerFound("same")
		synctest.Wait()
		if got := d.LookupCheckStats(); attempts != 2 || got.Started != 2 || got.Active != 0 {
			t.Fatalf("failure retained admission ownership: attempts=%d usage=%+v", attempts, got)
		}
	})
}

func TestDistinctPeerAdmissionStillHonorsConcurrencyBound(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		d := lookupCheckFixture(t, func(context.Context, peer.ID) (*pb.Message, error) { <-release; return &pb.Message{}, nil })
		d.peerFound("same")
		d.peerFound("other")
		d.peerFound("third")
		synctest.Wait()
		usage := d.LookupCheckStats()
		close(release)
		synctest.Wait()
		if usage.Active != 2 || usage.Started != 2 || usage.CapacityRejected != 1 {
			t.Fatalf("lookup-check concurrency escaped: %+v", usage)
		}
	})
}

func TestPeerAdmissionRetainsCapacityThroughRoutingTableHandoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		d := lookupCheckFixture(t, func(context.Context, peer.ID) (*pb.Message, error) { return &pb.Message{}, nil })
		d.addPeerToRTChan = make(chan peer.ID)
		d.peerFound("same")
		d.peerFound("other")
		synctest.Wait()
		d.peerFound("third")
		synctest.Wait()
		held := d.LookupCheckStats()
		first, second := <-d.addPeerToRTChan, <-d.addPeerToRTChan
		synctest.Wait()
		after := d.LookupCheckStats()
		if held.Active != 2 || held.Started != 2 || held.CapacityRejected != 1 || after.Active != 0 || first == second {
			t.Fatalf("handoff escaped admission capacity: held=%+v after=%+v peers=%q,%q", held, after, first, second)
		}
	})
}
