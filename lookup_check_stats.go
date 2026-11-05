package dht

// LookupCheckUsage has fixed dimensions. Active identities are bounded by the
// configured lookup-check concurrency; no completed identity history is kept.
type LookupCheckUsage struct {
	Started, Coalesced, CapacityRejected uint64
	Active                               int
}

// LookupCheckStats returns an owned snapshot without exposing peer IDs.
func (dht *IpfsDHT) LookupCheckStats() LookupCheckUsage {
	dht.lookupChecksLk.Lock()
	defer dht.lookupChecksLk.Unlock()
	return LookupCheckUsage{Started: dht.lookupCheckStarted, Coalesced: dht.lookupCheckCoalesced, CapacityRejected: dht.lookupCheckRejected, Active: len(dht.lookupChecks)}
}
