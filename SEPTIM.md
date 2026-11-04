# Septim maintenance branch

This branch moves existing dependency patches out of
[Septim](https://github.com/septimproject/septim). It introduces no new behavior
during extraction. Product policies and regression scenarios remain in Septim;
this fork contains generic library changes. The original module path is retained.

## Patch provenance

1. Source: [libp2p/go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht),
   version `v0.43.0`, commit `89f024ed30ce694352508eae183b366077eff3b1`.
   The fork retains upstream history, licenses, tests and contributor files.
2. Reason: native queries and liveness checks evict routing contacts after local
   resource admission errors. Under bounded RPC/dial budgets, healthy contacts
   disappear and subsequent lookups cannot recover from an empty table.
3. Patch: compare this branch against the upstream commit above.
   Admission refusals fail the attempt without evicting the contact. Actual
   transport/protocol failures still evict peers. No successful query, liveness
   timestamp, or address is fabricated. Wire formats, routing limits, validation
   and Amino defaults remain unchanged.
   Dial backoff is also a local refusal, including after an earlier budget error.
   Query and follow-up admission errors reach callers; zero successful responses
   return `ErrNoPeersQueried`. `GetValue` preserves these errors instead of
   reporting `not found`; its streaming `SearchValue` API stays compatible.
   Follow-up requests obey lookup concurrency. An opt-out from hard routing-peer
   connection protection retains priority tags; Septim uses it so routing state
   can outlive trimmed connections. Request writers optionally receive the current
   RPC context, including when a persistent stream is reused, allowing bounded
   pacing without duplicating the native message sender or framing.
   In the `IpfsDHT` engine used by Septim, positive value quorums stop at the requested response count, release results
   without waiting for other replicas, and cancel the search's remaining work.
   Zero quorum retains full-search selection and repair. Offline value searches
   read and validate local records without dialing, remote queries or repair;
   local storage errors remain visible. These are generic routing contracts,
   with no Septim namespace or trust policy in the dependency patch.
   `GetClosestPeersWithProgress` optionally reports responding peers within the
   existing Optimistic Provide individual-distance threshold. It preserves native
   lookup/follow-up completion and errors, falls back without a network-size
   estimate, and joins all callbacks before returning. Callbacks are concurrent
   and nonblocking; publication, bounded workers and receipt policy stay outside
   the dependency.
4. Septim marks budget refusals with libp2p's resource error and signals explicit
   overload resets. Regression tests in `internal/foundation` exercise native
   query and refresh paths as part of `make test`.

## Validation environment

1. Root regressions run through `go test -race ./...`; dependency-local tests must
   also be run when updating a snapshot. Root package discovery does not recurse
   into nested modules.
2. QUIC upstream tests require IPv6 loopback. A disposable user/network namespace
   with its loopback interface up provides it without changing the host.
3. Upstream `TestDialWorkerLoopTCPConnUpgradeWait` assumes that TCP to
   `192.0.0.170:10001` stays pending. Immediate host rejection also fails the
   unmodified pinned module. An isolated namespace with that destination routed
   locally and its outgoing SYNs dropped supplies the intended fixture. The full
   swarm suite passes with that fixture; no upstream test is skipped or relaxed.
4. The FullRT IP-diversity test installs its own routing table. It now uses the
   existing `newTestFullRT` fixture, whose crawler waits for cancellation, to
   prevent concurrent replacement by the automatically started crawler. The
   fixture closes the host and FullRT; all filter assertions remain in place.
   Production FullRT behavior is unchanged.

## Updating

Rebase this branch onto an explicitly selected upstream release, run affected
upstream tests and Septim regressions, then pin the resulting commit in Septim's
`go.mod`. Do not move an existing pin or add Septim application protocols here.
