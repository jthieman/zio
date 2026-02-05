# How pg.zig Uses zio: Library Design Analysis

## Overview

[pg.zig](https://github.com/lalinsky/pg.zig) is a fork of Karl Seguin's PostgreSQL
driver for Zig, modified by Lukas Lalinsky to use zio for networking and concurrency.
The library demonstrates a key design pattern where the zio runtime is passed through
the API as a handle, and the caller never needs to explicitly "spawn into" or "enter"
the runtime to use the library.

## The Illusion: No Spawn, No Enter

The README shows this usage pattern:

```zig
const rt = try zio.Runtime.init(allocator, .{});
defer rt.deinit();

var pool = try pg.Pool.init(allocator, rt, .{ ... });
defer pool.deinit();

var result = try pool.query("select id, name from users", .{});
defer result.deinit();
```

There is no `rt.spawn(...)`, no `rt.run(...)`, no entering any coroutine context.
You just call `pool.query()` and it works. How?

## The Key Mechanism: Main Thread as Implicit Executor

The answer lies in zio's `Runtime.init()` and the concept of a "main executor".

### 1. Runtime.init() Sets Up the Main Thread as an Executor

When you call `zio.Runtime.init(allocator, .{})`, the runtime:

1. Creates an `Executor` (the `main_executor` field) and calls `main_executor.init(self, 0)`
2. During `Executor.init()`, it sets the **thread-local** variable `Executor.current = self`
3. It also initializes a `main_task` — a pseudo-task that represents the main thread itself

This is the critical step. After `Runtime.init()` returns, the calling thread already
**is** an executor. The thread-local `Executor.current` is set, and the main thread
has a `main_task` that can participate in the async machinery.

See `runtime.zig:750`:
```zig
try self.main_executor.init(self, 0);
// Inside Executor.init():
//   Executor.current = self;  // thread-local set
```

### 2. Every Async Operation Goes Through yield() Which Handles Main Specially

When pg.zig calls something like `mutex.lock(rt)` or `stream.read(rt, buf, timeout)`,
these operations eventually need to suspend the current "task" and wait for a result.

The call chain is:
- `stream.read(rt, buf, timeout)` → `timedWaitForIo(rt, ...)` → `waitForIo(rt, ...)`
  → `Waiter.wait(...)` → `executor.yield(.preparing_to_wait, .waiting, ...)`
- `mutex.lock(rt)` → (slow path) → `Waiter.wait(...)` → `executor.yield(...)`

The `yield()` method in `Executor` (`runtime.zig:394-449`) has **two code paths**:

```zig
pub fn yield(self: *Executor, ...) {
    // ...
    if (is_main) {
        // Main: run the event loop until our main_task is marked ready
        self.run(.until_ready) catch |err| { ... };
    } else {
        // Spawned task: suspend coroutine, switch to another
        current_coro.yieldTo(&next_task.coro);
    }
}
```

When called from the main thread (`self.current_coroutine == null`), instead of
suspending a coroutine (there is no coroutine — it's just the main thread), it
**runs the event loop inline** via `self.run(.until_ready)`.

This is the entire secret: the main thread becomes a cooperative scheduler. When
it needs to wait for I/O, it runs the event loop, processing I/O completions and
other ready tasks, until the main thread's own operation completes.

### 3. The Event Loop Processes I/O Completions Inline

`Executor.run(.until_ready)` (`runtime.zig:484-558`) loops:
1. Process ready coroutines from the ready queue
2. Run the event loop (`self.loop.run(...)`) to check for I/O completions
3. When the I/O operation that the main thread is waiting for completes,
   the callback signals the `Waiter`, which sets `main_task.state = .ready`
4. The `run` method checks `main_task.state == .ready` and returns
5. Control returns to the caller (e.g., back to `pool.query()`)

## How pg.zig Uses This in Practice

### Connection Pool (pool.zig)

The pool stores a `*zio.Runtime` reference and uses it for:

| Operation | zio Primitive | What Happens on Main Thread |
|-----------|--------------|---------------------------|
| `pool.acquire()` | `mutex.lock(rt)`, `cond.timedWait(rt, ...)` | Runs event loop while waiting for lock/condition |
| `pool.release()` | `mutex.lockUncancelable(rt)`, `cond.signal(rt)` | Lock uses shield to ignore cancellation |
| Connection I/O | `stream.read(rt, ...)`, `stream.writeAll(rt, ...)` | Runs event loop while waiting for socket I/O |
| Background reconnect | `group.spawn(rt, ...)` | Spawns a real coroutine for reconnection |

### Network Stream (stream.zig)

The `PlainStream` wraps `zio.net.Stream` and passes `rt` to every operation:

```zig
pub fn writeAll(self: *const PlainStream, data: []const u8) !void {
    return self.stream.writeAll(self.rt, data, .none);
}

pub fn read(self: *const PlainStream, buf: []u8) !usize {
    return self.stream.read(self.rt, buf, self.timeout);
}
```

These ultimately call `waitForIo(rt, &op.c)` which:
1. Creates a stack-allocated `Waiter`
2. Submits the I/O operation to the event loop
3. Calls `waiter.wait(1, .allow_cancel)` which calls `executor.yield(...)`
4. On the main thread, this runs the event loop until the I/O completes

### Reconnector (pool.zig:253-305)

The reconnector is the one place where pg.zig does use real coroutine spawning:

```zig
fn reconnect(self: *Reconnector) !void {
    const prev = self.count.fetchAdd(1, .acq_rel);
    if (prev == 0) {
        try self.group.spawn(self.pool._rt, Reconnector.run, .{self});
    }
}
```

This spawns a background task via `zio.Group.spawn()`. The spawned task runs as a
real coroutine and uses `rt.sleep(retry_delay)` and connection operations. These
coroutines are executed by the event loop that the main thread drives when it
calls any blocking operation.

## Architecture Summary

```
Main Thread (your application code)
    |
    v
pool.query("SELECT ...")
    |
    v
pool.acquire()  →  mutex.lock(rt)  →  Waiter.wait()  →  executor.yield()
    |                                                          |
    |                               [main thread path]         |
    |                                                          v
    |                                              executor.run(.until_ready)
    |                                                          |
    |                                              +-----------+-----------+
    |                                              |                       |
    |                                    Process ready         Run event loop
    |                                    coroutines            (epoll/io_uring/kqueue)
    |                                    (e.g. reconnector)            |
    |                                                          I/O completion
    |                                                          signals Waiter
    |                                                                  |
    |                                              main_task.state = .ready
    |                                                          |
    |                                              executor.run() returns
    |                                                          |
    v  <-------------------------------------------------------+
conn.query(sql, values)
    |
    v
stream.writeAll(rt, data)  →  waitForIo(rt, ...)  →  [same pattern]
    |
    v
stream.read(rt, buf)  →  waitForIo(rt, ...)  →  [same pattern]
    |
    v
Result returned to caller
```

## Key Design Insights

1. **The runtime is a handle, not a context to enter.** `Runtime.init()` implicitly
   makes the calling thread an executor. There is no separate "enter" step because
   initialization *is* entering.

2. **Synchronous-looking API, async under the hood.** From the caller's perspective,
   `pool.query()` is a blocking call. Under the hood, it cooperatively yields to the
   event loop whenever it needs to wait for I/O, then resumes when the operation
   completes.

3. **Same code works in both contexts.** The `yield()` method transparently handles
   both the main-thread case (run event loop inline) and the coroutine case (suspend
   and switch). pg.zig code doesn't need to know which context it's in.

4. **Background tasks get free execution time.** When the main thread is waiting for
   I/O (e.g., waiting for a query response from Postgres), the event loop also
   processes ready coroutines. This means the reconnector coroutine gets to run
   during the main thread's I/O waits, without any explicit scheduling by pg.zig.

5. **Single-threaded by default.** The default `RuntimeOptions` uses
   `.executors = .exact(1)`, meaning everything runs on the main thread. The event
   loop is the only source of concurrency (multiplexing), not OS threads. Multi-executor
   mode can be opted into for actual parallelism.

6. **No hidden threads for basic usage.** Unlike Tokio (Rust) where you typically
   spawn a multi-threaded runtime, zio's default single-executor model means
   `pool.query()` from the main thread involves zero additional threads (beyond
   the thread pool for blocking operations like DNS resolution). All I/O
   multiplexing happens via the event loop (io_uring/epoll/kqueue).
