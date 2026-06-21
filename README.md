# kiro

A Kotlin/JVM implementation of the [BATMAN](https://www.open-mesh.org/projects/open-mesh/wiki/BATMANv4) (Better Approach To Mobile Ad-hoc Networking) routing protocol for heterogeneous, band-limited radio meshes.

The library runs entirely at the application layer — no kernel modules, no raw sockets — making it suitable for embedded JVM targets, LoRa networks, serial radio bridges, or any environment where the physical link is exposed as a simple send/receive primitive.

---

## Features

- **Distributed hop-by-hop routing** via periodic OGM (Originator Message) broadcasts
- **Jitter-based relay suppression** — in a dense subnet of N neighbours, expected relays ≈ ln(N) instead of N−1
- **Beacon-driven multicast spanning trees** — members build the tree themselves; no network-wide flooding
- **Pull-model transmit queue** with rule-based ordering and per-handle cancellation, replacement and reordering
- **Compact wire format** — 6 bytes for an OGM, 8 bytes for a beacon, up to 255-byte payloads
- **Fully coroutine-native** — every loop suspends instead of polling; slow radios never block fast ones

---

## Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("kiro:kiro:1.0-SNAPSHOT")
}
```

Requirements: **Kotlin 2.0+**, **JVM 17+**, `kotlinx-coroutines-core 1.8+`.

---

## Quick start

```kotlin
// 1. Implement the Link interface for your physical medium.
val radio: Link = object : Link {
    override val id = "lora0"
    override val ogmInterval = 5.seconds   // drives routing metric and relay timing
    override suspend fun broadcast(frame: ByteArray) = radio.send(frame)
    override val frames: Flow<ByteArray> = radioReceiveFlow()
}

// 2. Create a router and start it inside a CoroutineScope.
val router = KiroRouter(selfId = 0x001u, links = listOf(radio))
router.start(scope)

// 3. Send unicast data to another node.
router.send(dstId = 0x002u, payload = "hello".encodeToByteArray())

// 4. Receive unicast data.
router.incomingData.collect { (srcId, payload) ->
    println("from $srcId: ${payload.decodeToString()}")
}
```

---

## Core concepts

### NodeId

A `UShort` (16-bit unsigned) identifier, unique within the mesh. Only the lower 12 bits are transmitted on the wire, limiting deployments to 4 095 nodes — sufficient for virtually all mesh use cases and saving one byte per ID field.

### Link

The only interface the library requires you to implement. It abstracts a single broadcast radio interface — LoRa, serial, UDP multicast, Bluetooth, etc.

```kotlin
interface Link {
    val id: String               // unique name used as a map key
    val ogmInterval: Duration    // how often this node emits OGMs on this link
    suspend fun broadcast(frame: ByteArray)
    val frames: Flow<ByteArray>  // cold Flow of every received raw frame
}
```

`ogmInterval` doubles as a routing metric: a link with a longer interval accumulates more TTL decay over multiple hops, so the protocol naturally prefers faster links when an alternative exists.

### KiroRouter

One instance per node. Orchestrates OGM emission, relay suppression, route table maintenance, multicast tree building, and frame forwarding.

```kotlin
KiroRouter(
    selfId: NodeId,
    links: List<Link>,
    txQueue: TxQueue = TxQueue(),
    staleThreshold: Duration = 90.seconds,
    neighborPurgeMultiplier: Int = 3
)
```

| Parameter | Effect |
|---|---|
| `staleThreshold` | How long a multicast branch is kept without a refreshing beacon before eviction |
| `neighborPurgeMultiplier` | Route is evicted after this many missed OGM cycles on its link |

Call `router.start(scope)` once before using any other method. All protocol loops are launched inside the given scope and cancelled when it is cancelled.

---

## Unicast

```kotlin
// Send
router.send(dstId = 0x042u, payload = bytes)

// Receive
router.incomingData.collect { (srcId, payload) -> ... }
```

`send` looks up the best next hop in the routing table and enqueues a `DataFrame`. If no route is known the frame is silently dropped. Forwarding nodes decrement TTL (initial value 15) and drop frames that reach zero.

---

## Multicast

Multicast is built on a per-group spanning tree. Members send periodic **beacons** toward the group root; every relay node records which links beacons arrive on and which link leads toward the root. Multicast frames then travel the tree in both directions without flooding.

### Creating and joining a group

Group identifiers are opaque 20-bit values chosen by the application. The node designated as root joins with `roots = listOf(selfId)`; members join with `roots` pointing at the root:

```kotlin
// Root node — beacons flow toward it, so no beacon loop is started
val gid = GroupId(0xA01u)   // application-assigned opaque id
router.joinGroup(gid, roots = listOf(router.selfId))

// Member node — launches a beacon loop toward the root
router.joinGroup(gid, roots = listOf(rootId), beaconInterval = 30.seconds)
```

### Sending and receiving

```kotlin
// Send to all members of a group
router.sendMulticast(gid, payload = bytes)

// Receive multicast messages
router.incomingMulticast.collect { msg ->
    println("${msg.srcId} → ${msg.groupId}: ${msg.payload.decodeToString()}")
}
```

### Root failover

When the primary root goes offline, members independently select the first alternative root with a known route from the `roots` list passed to `joinGroup`. Because all nodes share the same OGM view they converge on the same root without coordination. The active root is carried in every `BeaconFrame` so relay nodes need no local root list.

---

## Transmit queue

All outgoing frames pass through `TxQueue` — a sorted, pull-model queue shared by all link TX coroutines. Frames are dequeued by whichever link's TX loop calls `pollFor` first.

### Ordering

Ordering is controlled by a composable list of `Rule` objects (each a `Comparator<TxEntry>`). The defaults are:

1. `controlFirst` — OGM / BEACON before DATA / MULTICAST
2. `userPriorityFirst` — lower `userPriority` wins (default 0; client-controlled)
3. `olderFirst` — FIFO within a priority class
4. `insertionOrderFirst` — strict total order tiebreaker

Custom rule lists can be injected into `TxQueue`:

```kotlin
val q = TxQueue(listOf(
    compareBy { when (it.flavor) { PacketFlavor.OGM -> 0; PacketFlavor.BEACON -> 1; else -> 2 } },
    olderFirst,
    insertionOrderFirst
))
```

### TxHandle

`enqueue` returns a `TxHandle` that lets you track and mutate a queued entry before it is transmitted:

```kotlin
val handle: TxHandle = txQueue.enqueue(entry)

// Non-suspending notification — fires on whichever thread transmits the frame
handle.onSent { log("frame transmitted") }

// Suspending — use inside a coroutine
launch { handle.awaitSent(); doNextStep() }

// Mutations — all return false if the entry was already sent
handle.cancel()                    // remove from queue
handle.replace(newBytes)           // swap frame bytes, keep queue position
handle.swap(otherHandle)           // exchange positions with another queued entry
```

`swap` exchanges `userPriority` and `enqueuedAt` between two entries, inverting their relative order. It has no effect across flavor boundaries — a control frame always transmits before a data frame regardless of a swap.

---

## Timing configuration

Use `recommendedConfig` to derive `ogmInterval` and `neighborPurgeMultiplier` from link bandwidth:

```kotlin
val cfg = recommendedConfig(
    linkBandwidthBps = 50_000L,  // 50 kbps
    expectedNodes = 40,          // conservative upper bound on network size
    dataFraction = 0.5           // reserve at least 50 % for application data
)

// cfg.ogmInterval           ≈ 77 ms
// cfg.neighborPurgeMultiplier = 5
// cfg.purgeTimeout          ≈ 385 ms (= interval × multiplier)
```

The formula: `ogmInterval = N × 48 bits / ((1 − dataFraction) × linkBandwidthBps)`, floored at `minOgmInterval` (default 5 s). The purge multiplier is chosen so `purgeTimeout ≈ 60 s` across all link speeds, clamped to [3, 5].

| Link speed | Nodes | ogmInterval | purgeMultiplier |
|---|---|---|---|
| 50 bps | 40 | ~77 s | 3 |
| 500 bps | 40 | ~7.7 s | 5 |
| 5 kbps | 40 | ~5 s (floor) | 5 |
| 50 kbps | 40 | ~5 s (floor) | 5 |
| ≥ 1 Gbps | any | 5 s (floor) | 5 |

---

## Wire format

All frames are bit-packed big-endian. Byte 0 always carries the 4-bit type tag in the high nibble and the high nibble of the first 12-bit node ID in the low nibble.

| Frame | Size | Notes |
|---|---|---|
| `OgmFrame` | 6 bytes | originatorId(12b) + senderId(12b) + ttl(4b) + seqNum(16b) |
| `DataFrame` | 7 + n bytes | nextHop(12b) + src(12b) + dst(12b) + ttl(4b) + varint(len) + payload |
| `BeaconFrame` | 8 bytes | nextHop(12b) + src(12b) + groupId(20b) + activeRoot(12b) |
| `MulticastFrame` | 8 + n bytes | src(12b) + groupId(20b) + ttl(4b) + seqNum(16b) + varint(len) + payload |

Payload lengths use a 7-bit continuation varint: lengths ≤127 cost 1 byte, ≤16383 cost 2 bytes, ≤2097151 cost 3 bytes. No upper limit is imposed at the codec layer.

`Codec.encode` and `Codec.decode` handle serialisation. `decode` returns `null` for malformed or unknown frames; the router silently drops them.

---

## Design notes

**Why OGM interval as a metric?** A slower link emits OGMs less frequently, so OGMs from distant nodes arrive with a lower TTL. The routing table stores the best (highest) TTL seen, which naturally reflects the fewest hops over the fastest links. No extra metric field is needed.

**Why pull-model TxQueue?** Each link's TX coroutine suspends in `pollFor` until a frame is available. A slow LoRa radio never delays a fast Ethernet link. No thread sleeps, no polling timers.

**Why separate upstream and downstream links in MulticastTree?** It enables two optimisations: leaf suppression (a relay whose downstream members are actively beaconing does not need to send its own beacon) and immediate upstream replacement (when the best route to the root changes, the old upstream link is discarded atomically, preventing duplicate multicast transmissions during reroutes).

**Why activeRoot carried in BeaconFrame?** Relay nodes need no local configuration. The active root is embedded in every beacon, so any node can forward toward the correct root without knowing the group's root list.
