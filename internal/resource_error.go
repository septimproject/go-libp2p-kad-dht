package internal

import (
	"errors"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

// IsAdmissionError reports local admission failures and explicit overload
// resets. Neither is evidence that a peer stopped serving the DHT. The caller
// still fails this attempt; it must not count the peer as successfully queried.
func IsAdmissionError(err error) bool {
	// Swarm also backs off addresses after a locally refused dial. A later
	// backoff is not a new observation of the remote peer's health.
	if errors.Is(err, network.ErrResourceLimitExceeded) || errors.Is(err, swarm.ErrDialBackoff) {
		return true
	}
	var stream *network.StreamError
	if errors.As(err, &stream) {
		return stream.ErrorCode == network.StreamResourceLimitExceeded || stream.ErrorCode == network.StreamRateLimited
	}
	var conn *network.ConnError
	return errors.As(err, &conn) && (conn.ErrorCode == network.ConnResourceLimitExceeded || conn.ErrorCode == network.ConnRateLimited || conn.ErrorCode == network.ConnGarbageCollected)
}
