package dht

import (
	"context"
	"errors"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/internal/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ks "github.com/whyrusleeping/go-keyspace"
	"go.opentelemetry.io/otel/trace"
	"gonum.org/v1/gonum/mathext"
)

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a slice of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error
// along with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	return dht.getClosestPeers(ctx, key, dht.pmGetClosestPeers(key))
}

// GetClosestPeersWithProgress reports responding peers that satisfy Optimistic
// Provide's individual-distance threshold while ordinary discovery continues.
// Without a network-size estimate it behaves like GetClosestPeers. The callback
// may run concurrently and must not block; it is never called after return.
// Final peers, termination, follow-up, cancellation and errors are unchanged.
func (dht *IpfsDHT) GetClosestPeersWithProgress(ctx context.Context, key string, onPeer func(peer.ID)) ([]peer.ID, error) {
	query := dht.pmGetClosestPeers(key)
	if onPeer == nil {
		return dht.getClosestPeers(ctx, key, query)
	}
	size, err := dht.nsEstimator.NetworkSize()
	if err != nil || size <= 0 {
		return dht.getClosestPeers(ctx, key, query)
	}
	threshold := mathext.GammaIncRegInv(float64(dht.bucketSize), 1-optProvIndividualThresholdCertainty) / float64(size)
	target := ks.XORKeySpace.Key([]byte(key))
	return dht.getClosestPeers(ctx, key, func(ctx context.Context, id peer.ID) ([]*peer.AddrInfo, error) {
		peers, err := query(ctx, id)
		if err == nil && netsize.NormedDistance(id, target) <= threshold {
			onPeer(id)
		}
		return peers, err
	})
}

func (dht *IpfsDHT) getClosestPeers(ctx context.Context, key string, query queryFn) ([]peer.ID, error) {
	ctx = internal.DefaultQueryPurpose(ctx, internal.QueryClosestLookup)
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, errors.New("can't lookup empty key")
	}

	// TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, query, func(*qpeerset.QueryPeerset) bool { return false })
	if err != nil {
		if lookupRes != nil {
			return lookupRes.peers, err
		}
		return nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		if err != netsize.ErrWrongNumOfPeers || dht.routingTable.Size() > dht.bucketSize {
			// Don't warn if we have a wrong number of peers and the routing table is
			// small because the network may simply not have enough peers.
			logger.Warnf("network size estimator track peers: %s", err)
		}
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.RecordNetworkSize(dht.ctx, int64(ns))
	}

	// Reset the refresh timer for this key's bucket since we've just
	// successfully interacted with the closest peers to key
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, nil
}

// pmGetClosestPeers is the protocol messenger version of the GetClosestPeer queryFn.
func (dht *IpfsDHT) pmGetClosestPeers(key string) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}
