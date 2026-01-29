# io_uring Feature Implementation Ranking for ZIO

This document analyzes the missing io_uring features in ZIO and ranks them by implementation effort, value for high-performance messaging, and architectural fit.

## Executive Summary

| Rank | Feature | Effort | Value | Priority Score |
|------|---------|--------|-------|----------------|
| 1 | **Multishot Accept** | Low | Medium | ⭐⭐⭐⭐⭐ |
| 2 | **SQE Linking** | Low-Medium | Medium | ⭐⭐⭐⭐ |
| 3 | **Registered Buffers** | Medium | High | ⭐⭐⭐⭐ |
| 4 | **Zero-Copy Send (SEND_ZC)** | Medium-High | High | ⭐⭐⭐ |
| 5 | **Multishot Recv** | High | Medium | ⭐⭐⭐ |
| 6 | **Provided Buffer Rings** | High | Medium-High | ⭐⭐ |
| 7 | **Zero-Copy Receive (ZC Rx)** | Very High | High | ⭐ |

---

## ZIO Architecture Overview

Understanding ZIO's architecture is critical for assessing implementation effort.

### Layer Structure

```
┌─────────────────────────────────────────┐
│  High-Level API (zio.net.Stream, etc.)  │  ← User-facing
├─────────────────────────────────────────┤
│  Runtime (spawn, select, channels)      │  ← Task scheduling
├─────────────────────────────────────────┤
│  ev/loop.zig (Loop, LoopState)          │  ← Completion management
├─────────────────────────────────────────┤
│  ev/completion.zig (Op types)           │  ← Operation definitions
├─────────────────────────────────────────┤
│  ev/backends/io_uring.zig               │  ← io_uring specifics
└─────────────────────────────────────────┘
```

### Key Abstractions

1. **Completion**: Core struct representing an async operation
   - Has `op: Op` enum identifying the operation type
   - Contains `internal` field for backend-specific data
   - Single-shot: one SQE → one CQE → one callback

2. **Loop**: Manages submission and completion
   - `add()` submits operations
   - `tick()` polls for completions
   - Single callback per completion (no multi-CQE support)

3. **Backend**: Platform-specific implementation
   - `submit()` prepares SQEs
   - `poll()` retrieves CQEs
   - `cancel()` cancels in-flight operations

### Critical Constraint

**ZIO assumes 1:1 SQE:CQE mapping.** The completion model expects exactly one CQE per submitted operation. This is the primary architectural challenge for multishot operations.

---

## Feature Analysis

### 1. Multishot Accept (⭐⭐⭐⭐⭐)

**Current**: Each `accept()` call submits one SQE, gets one CQE, returns one connection.

**Goal**: One SQE generates multiple CQEs, each representing an accepted connection.

#### Implementation Effort: LOW

**Why it's easy:**
- Accept is inherently stateless - each CQE is independent
- No buffer management needed
- The existing `NetAccept` completion can be reused

**Changes Required:**

1. **completion.zig** (~10 lines)
   ```zig
   pub const NetAccept = struct {
       // ... existing fields ...
       multishot: bool = false,  // NEW
   };
   ```

2. **io_uring.zig** (~20 lines)
   ```zig
   // In submit() for .net_accept:
   sqe.prep_accept(data.handle, data.addr, data.addr_len, 0);
   if (data.multishot) {
       sqe.flags |= linux.IOSQE_MULTISHOT;
   }

   // In poll(), check CQE flags:
   if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
       // Don't decrement inflight_io, operation continues
       // Clone completion for callback, keep original for next CQE
   }
   ```

3. **High-level API** (~15 lines)
   - Add `Server.acceptMultishot()` that returns an iterator/channel

**Estimated Lines Changed**: ~50-80
**Risk**: Low - isolated change, easy to test
**Kernel Version**: 5.19+

#### Value for Messaging

- **8% performance improvement** in connection-heavy workloads
- Reduces SQE submission overhead for servers handling many short-lived connections
- Less valuable for long-lived connections (typical in messaging)

---

### 2. SQE Linking (⭐⭐⭐⭐)

**Current**: Each operation is independent.

**Goal**: Chain operations so they execute sequentially (e.g., read → process → write).

#### Implementation Effort: LOW-MEDIUM

**Why it's relatively easy:**
- No changes to completion model - still 1:1 SQE:CQE
- Just need to set `IOSQE_IO_LINK` flag on SQEs
- Existing completion flow handles results

**Changes Required:**

1. **completion.zig** (~5 lines)
   ```zig
   pub const Completion = struct {
       // ... existing fields ...
       link_next: ?*Completion = null,  // For linked chains
   };
   ```

2. **loop.zig** (~30 lines)
   ```zig
   pub fn addLinked(self: *Loop, completions: []*Completion) void {
       for (completions, 0..) |c, i| {
           if (i < completions.len - 1) {
               c.link_next = completions[i + 1];
           }
           self.addInternal(c);
       }
   }
   ```

3. **io_uring.zig** (~15 lines)
   ```zig
   // In submit():
   if (c.link_next != null) {
       sqe.flags |= linux.IOSQE_IO_LINK;
   }
   ```

**Estimated Lines Changed**: ~50-70
**Risk**: Low-Medium - need careful error handling for chain failures

#### Value for Messaging

- Useful for atomic read-modify-write patterns
- Can chain recv → process → send for request-response
- Reduces round-trips for multi-step operations
- **Medium value** - most messaging patterns don't need strict ordering

---

### 3. Registered Buffers (⭐⭐⭐⭐)

**Current**: Each operation passes buffer pointers; kernel maps them per-operation.

**Goal**: Pre-register buffers to eliminate per-operation mapping overhead.

#### Implementation Effort: MEDIUM

**Why it's medium:**
- Need new registration API at Loop/Backend level
- Operations need to reference buffers by index, not pointer
- Requires buffer lifecycle management
- Cross-platform concern: only benefits io_uring

**Changes Required:**

1. **New file: ev/buffer_registry.zig** (~100 lines)
   ```zig
   pub const BufferRegistry = struct {
       buffers: [][]u8,
       registered: bool = false,

       pub fn init(allocator: Allocator, count: usize, size: usize) !BufferRegistry { ... }
       pub fn register(self: *BufferRegistry, ring: *IoUring) !void { ... }
       pub fn unregister(self: *BufferRegistry, ring: *IoUring) void { ... }
       pub fn get(self: *BufferRegistry, index: u16) []u8 { ... }
   };
   ```

2. **io_uring.zig** (~40 lines)
   ```zig
   buffer_registry: ?*BufferRegistry = null,

   pub fn registerBuffers(self: *Self, registry: *BufferRegistry) !void {
       const iovecs = // convert registry.buffers to iovec array
       _ = try linux.io_uring_register(
           self.ring.fd,
           .REGISTER_BUFFERS,
           @ptrCast(iovecs.ptr),
           @intCast(iovecs.len),
       );
       registry.registered = true;
       self.buffer_registry = registry;
   }
   ```

3. **completion.zig** (~20 lines)
   ```zig
   pub const NetRecv = struct {
       // ... existing fields ...
       fixed_buffer_index: ?u16 = null,  // NEW: use registered buffer
   };
   ```

4. **io_uring.zig submit()** (~30 lines)
   ```zig
   // Use prep_read_fixed / prep_write_fixed when fixed_buffer_index is set
   if (data.fixed_buffer_index) |idx| {
       sqe.prep_read_fixed(fd, buf, offset, idx);
   } else {
       sqe.prep_readv(fd, iovecs, offset);
   }
   ```

**Estimated Lines Changed**: ~200-250
**Risk**: Medium - buffer lifecycle management, cross-platform API design

#### Value for Messaging

- **High value** for throughput-critical paths
- Eliminates page pinning overhead per operation
- Most beneficial when same buffers are reused repeatedly
- Essential foundation for zero-copy operations

---

### 4. Zero-Copy Send (SEND_ZC) (⭐⭐⭐)

**Current**: Data is copied from user buffers to kernel socket buffers.

**Goal**: Send directly from user memory without copying.

#### Implementation Effort: MEDIUM-HIGH

**Why it's harder:**
- Requires **two CQEs per operation**: one for send completion, one for buffer release
- Need to track "buffer still in use" state
- User must not modify buffer until notified
- Requires registered buffers (dependency)

**Changes Required:**

1. **Depends on**: Registered Buffers implementation

2. **completion.zig** (~30 lines)
   ```zig
   pub const NetSendZC = struct {
       c: Completion,
       // ... send fields ...

       // ZC-specific state
       send_completed: bool = false,
       buffer_released: bool = false,
       notif_seq: u32 = 0,  // For matching notification CQE
   };
   ```

3. **io_uring.zig** (~80 lines)
   ```zig
   // New operation handling
   .net_send_zc => {
       sqe.opcode = .SEND_ZC;
       sqe.flags |= linux.IOSQE_CQE_SKIP_SUCCESS;  // Only get notif CQE
       // OR handle both CQEs...
   }

   // In poll(), handle notification CQE:
   if (cqe.flags & linux.IORING_CQE_F_NOTIF != 0) {
       // Find completion by notif_seq
       // Mark buffer_released = true
       // Complete only when both flags set
   }
   ```

4. **loop.zig** (~40 lines)
   - Track pending ZC notifications
   - Defer completion until buffer released

5. **High-level API** (~50 lines)
   ```zig
   pub fn sendZeroCopy(self: *Stream, rt: *Runtime, data: []const u8) !void {
       // Must use registered buffer
       // Returns when buffer can be reused
   }
   ```

**Estimated Lines Changed**: ~250-350
**Risk**: High - dual CQE handling breaks 1:1 model, complex state machine

#### Value for Messaging

- **200%+ improvement** over MSG_ZEROCOPY for large messages
- Most valuable for large message payloads (>16KB)
- Less valuable for small messages (copy overhead is small)
- **High value** for pub/sub with large payloads

---

### 5. Multishot Recv (⭐⭐⭐)

**Current**: Each recv submits one SQE, gets one CQE with data.

**Goal**: One SQE generates multiple CQEs as data arrives.

#### Implementation Effort: HIGH

**Why it's hard:**
- **Breaks 1:1 completion model** fundamentally
- Requires provided buffers (kernel picks buffer per CQE)
- Need to handle variable buffer allocation
- Completion callback model doesn't fit (multiple callbacks per submission)

**Changes Required:**

1. **Depends on**: Provided Buffer Rings (see below)

2. **Major architectural change to completion model** (~200 lines)
   ```zig
   pub const CompletionMode = enum {
       single_shot,
       multi_shot,
   };

   pub const Completion = struct {
       // ... existing ...
       mode: CompletionMode = .single_shot,

       // For multishot: called for each CQE
       multi_callback: ?*const fn(*Completion, result: anytype) void = null,
   };
   ```

3. **io_uring.zig** (~100 lines)
   - Handle `IORING_CQE_F_MORE` flag
   - Extract buffer ID from CQE
   - Call multi_callback without marking complete
   - Only complete when `F_MORE` not set

4. **loop.zig** (~80 lines)
   - Track multishot completions separately
   - Handle cancellation of multishot operations
   - Buffer management integration

5. **High-level API** (~100 lines)
   ```zig
   pub fn recvMultishot(self: *Stream, rt: *Runtime) MultishotReceiver {
       // Returns iterator-like object
       // Each .next() returns buffer from latest CQE
   }
   ```

**Estimated Lines Changed**: ~500-600
**Risk**: Very High - fundamental architecture change

#### Value for Messaging

- **~8% performance improvement** in recv-heavy workloads
- Reduces SQE submission for streaming data
- Most valuable for high-message-rate scenarios
- **Medium value** - improvement is modest compared to implementation cost

---

### 6. Provided Buffer Rings (⭐⭐)

**Current**: Application provides specific buffer for each recv.

**Goal**: Kernel picks buffer from ring; application returns it after processing.

#### Implementation Effort: HIGH

**Why it's hard:**
- New buffer management abstraction needed
- Ring buffer with kernel-shared state
- Buffer ID extraction from CQE
- Lifecycle: allocate → provide → kernel picks → user processes → return
- Required for multishot recv

**Changes Required:**

1. **New file: ev/buffer_ring.zig** (~200 lines)
   ```zig
   pub const BufferRing = struct {
       ring: *linux.io_uring_buf_ring,
       buffers: [][]u8,
       buffer_size: usize,
       group_id: u16,
       head: u16,

       pub fn init(allocator: Allocator, ring: *IoUring, group_id: u16, count: u16, size: usize) !BufferRing { ... }
       pub fn deinit(self: *BufferRing, ring: *IoUring) void { ... }
       pub fn returnBuffer(self: *BufferRing, buf_id: u16) void { ... }
   };
   ```

2. **io_uring.zig** (~60 lines)
   ```zig
   pub fn setupBufferRing(self: *Self, group_id: u16, count: u16) !*BufferRing { ... }

   // In poll():
   if (cqe.flags & linux.IORING_CQE_F_BUFFER != 0) {
       const buf_id = @intCast(u16, cqe.flags >> 16);
       const buffer = self.buffer_rings[group_id].getBuffer(buf_id);
       // Pass buffer to completion
   }
   ```

3. **completion.zig** (~30 lines)
   ```zig
   pub const NetRecv = struct {
       // ... existing ...
       buffer_group: ?u16 = null,  // Use provided buffers from this group

       // Result includes buffer reference
       result_buffer: ?struct { data: []u8, id: u16 } = null,
   };
   ```

**Estimated Lines Changed**: ~350-400
**Risk**: High - complex kernel interaction, memory mapping

#### Value for Messaging

- Required for multishot recv
- Reduces buffer allocation overhead
- **Medium-High value** as enabler for other features

---

### 7. Zero-Copy Receive (ZC Rx) (⭐)

**Current**: Kernel copies received data to user buffers.

**Goal**: Packet data received directly into userspace memory.

#### Implementation Effort: VERY HIGH

**Why it's extremely hard:**
- Requires specific NIC hardware support (header/data split)
- Complex kernel configuration (outside io_uring setup)
- Memory region registration with specific alignment
- New kernel API (added in 6.x, still evolving)
- ZIO would need to detect/configure NIC capabilities

**Changes Required:**

1. **Hardware detection** (~100 lines)
   - Query NIC capabilities via ethtool/netlink
   - Configure header/payload split

2. **Memory region setup** (~150 lines)
   - Allocate aligned memory regions
   - Register with io_uring using new APIs
   - Handle IORING_OP_RECV_ZC

3. **New receive path** (~200 lines)
   - Different completion handling
   - Reference counting for shared buffers
   - Integration with network stack

4. **io_uring.zig** (~150 lines)
   - New SQE prep functions
   - Memory region management
   - Special CQE handling

**Estimated Lines Changed**: ~600+
**Risk**: Very High - hardware dependent, evolving kernel API

#### Value for Messaging

- **50% reduction in memory bandwidth** for receive path
- **High value** for receive-heavy workloads
- Limited hardware support currently
- Kernel API still maturing

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks)

1. **Multishot Accept** - Low effort, immediate benefit for servers
2. **SQE Linking** - Low effort, useful primitive

### Phase 2: Foundation (2-4 weeks)

3. **Registered Buffers** - Required for later features
4. **Provided Buffer Rings** - Required for multishot recv

### Phase 3: Advanced (4-8 weeks)

5. **Zero-Copy Send** - High value for large messages
6. **Multishot Recv** - Requires architecture changes

### Phase 4: Future (When kernel/hardware mature)

7. **Zero-Copy Receive** - Wait for broader hardware support

---

## Recommendations for ZeroMQ-like Library

For building a high-performance messaging library on ZIO:

### Immediate Use (No Changes Needed)

- **Vectored I/O** - Already supported via `sendVec`/`recvVec`
- **Channels** - Excellent for inproc transport
- **Multi-threaded runtime** - Good for CPU parallelism
- **Cancellation** - Clean shutdown support

### Worth Implementing

| Feature | Why | When |
|---------|-----|------|
| Registered Buffers | Foundation for perf | Before going to production |
| Zero-Copy Send | Large message perf | If message size > 16KB typical |
| Multishot Accept | Server scalability | High connection churn scenarios |

### Probably Not Worth It

| Feature | Why Not |
|---------|---------|
| Multishot Recv | High effort, modest gain, architecture impact |
| ZC Rx | Hardware requirements, evolving API |
| SQE Linking | Limited messaging use cases |

### Alternative Approaches

Instead of implementing missing io_uring features, consider:

1. **Use larger buffers** - Amortize syscall overhead
2. **Batch operations** - Use Groups to submit many operations
3. **Buffer pools** - Reuse allocations in userspace
4. **Message coalescing** - Combine small messages before send

---

## References

- [io_uring and networking in 2023](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023)
- [Zero-copy network transmission with io_uring](https://lwn.net/Articles/879724/)
- [io_uring multishot recv](https://lwn.net/Articles/899498/)
- [io_uring zero copy Rx kernel docs](https://docs.kernel.org/networking/iou-zcrx.html)
- [Zig stdlib io_uring](https://github.com/ziglang/zig/blob/master/lib/std/os/linux.zig)
- [zig-aio project](https://ziggit.dev/t/zig-aio-lightweight-abstraction-over-io-uring-and-coroutines/4767)
