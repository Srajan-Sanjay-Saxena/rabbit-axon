# RabbitMQ Service — Internals & Design Decisions

This document covers the internal design decisions, nuances, and concepts behind this library. Not a usage guide — read this to understand **why** things are built the way they are.

---

## Architecture

```
IRabbitConnection (interface)
  ├── RabbitSingleConnectionHandler   — one connection, reconnect logic
  └── RabbitConnectionPoolHandler     — pool of single connections, acquire/release

ChannelManager (static utility)
  — createChannel, createConfirmChannel, closeChannels
  — used directly by producer, consumer, exchange — never instantiated

RabbitMqQueueExchange (abstract)
  — exchange/queue setup, one channel per operation, closed after

RabbitProducer
  — persistent channel reuse, buffering, backpressure, confirm support

RabbitConsumer
  — multi-worker, prefetch, retry with exponential backoff
```

---

## IRabbitConnection

An interface, not an abstract class. Both connection handlers implement it:

```typescript
interface IRabbitConnection {
  ConnectToService(): Promise<void>;
  onReconnect(cb: () => Promise<void>): void;
  gracefulShutdown(): Promise<void>;
}
```

Connection handlers are responsible for **connection lifecycle only** — connecting, reconnecting, shutting down. They do not create or manage channels. That is the responsibility of the classes that need them (producer, consumer, exchange).

---

## connection.on("error") vs connection.on("close")

These are two distinct events on the AMQP connection and serve different purposes:

**`connection.on("error")`**
- Fires when something goes wrong on the connection — protocol error, heartbeat timeout, broker sends an error frame
- The connection is still technically open at this point
- amqplib will automatically emit `close` right after an error
- You only log here — do not attempt reconnect yet

**`connection.on("close")`**
- Fires when the connection is actually gone — whether after an error or a clean close
- This is where reconnect logic lives
- Always fires after `error`, but can also fire without `error` (network cut, broker killed, graceful shutdown)

```
broker crashes
  → connection.on("error") fires  → log it
  → connection.on("close") fires  → reconnect

network cut
  → connection.on("close") fires  → reconnect (no error event)

gracefulShutdown called
  → connection.on("close") fires  → isShuttingDown=true → do nothing
```

**Connection drop vs graceful shutdown** are completely different scenarios:

- Connection drop — unexpected, broker side or network. `isShuttingDown = false` → `Reconnect()` fires. Channels are gone, `conn` is stale.
- Graceful shutdown — intentional, your side. `isShuttingDown = true` → no reconnect. Connection closed cleanly.

Do not call `gracefulShutdown` on a dropped connection — the connection is already gone and it will throw. The `close` event handler checks `isShuttingDown` to distinguish between the two cases.

---

## RabbitSingleConnectionHandler vs RabbitConnectionPoolHandler — one `amqp.ChannelModel` connection. Handles auto-reconnect with exponential backoff. Exposes `rabbitConnection` getter for the raw connection.

`RabbitConnectionPoolHandler` — holds multiple `RabbitSingleConnectionHandler` instances. Tracks `connections[]` (all) and `availableConnections[]` (free). `acquire()` pops one out, `release()` puts it back. `ConnectToService()` spins up all connections in the pool.

Why pool? Under high throughput, a single AMQP connection can become a bottleneck. Multiple connections allow true parallelism at the TCP level.

---

## ChannelManager — Static Utility

`ChannelManager` is a pure static utility class — never instantiated. Used directly wherever a channel is needed:

```typescript
const channel = await ChannelManager.createChannel(conn, onCloseCb);
```

It attaches `error` and `close` event handlers on every channel it creates. The `onCloseCb` is how the caller reacts to a channel closing — e.g. setting `this.channel = null` in the producer so it gets recreated on next use.

`closeChannels` closes all channels and collects errors via `AggregateError` — it never stops early on a single failure, all channels get a close attempt.

---

## amqp.ChannelModel — The Connection Type

`amqp.connect()` returns `Promise<amqp.ChannelModel>` — not `amqp.Connection`. Despite the confusing name, `ChannelModel` is the actual connection object in amqplib. `amqp.Connection` is a separate interface that is not returned by `connect()`.

---

## Exchange — Channel Per Operation

Exchange setup (`assertExchange`, `assertQueue`, `bindQueue` etc.) is a one-time operation. There is no reason to hold a channel open for the lifetime of the exchange object.

Each method uses `withChannel()` — creates a fresh channel, executes the operation, closes it in `finally`:

```typescript
private async withChannel<T>(fn: (ch: amqp.Channel) => Promise<T>): Promise<T> {
  const channel = await ChannelManager.createChannel(this.conn, () => {});
  try {
    return await fn(channel);
  } finally {
    await channel.close();
  }
}
```

This avoids holding open channels that are no longer needed after setup.

---

## Producer — Channel Reuse

Unlike exchange, the producer reuses its channel across publishes to avoid the overhead of creating and closing a channel on every message.

Channels are stored as instance variables and lazily initialized:

```typescript
private channel: amqp.Channel | null = null;

private async getChannel(): Promise<amqp.Channel> {
  if (!this.channel)
    this.channel = await ChannelManager.createChannel(this.conn, () => { this.channel = null; });
  return this.channel;
}
```

When the channel closes (connection drop, broker restart), `onCloseCb` sets it back to `null`. Next `publish()` call recreates it automatically.

---

## Normal Channel vs Confirm Channel

**Normal channel** — fire and forget. `publish()` returns immediately with no guarantee the broker received the message.

**Confirm channel** — broker sends an ack/nack for every message. `waitForConfirms()` blocks until the broker responds.

```
Normal:   publish() → done (no guarantee)

Confirm:  publish() → waitForConfirms() → ack ✓ (broker got it)
                                        → nack ✗ (broker rejected it) → buffer for retry
```

Use normal channel for high throughput where occasional loss is acceptable. Use confirm channel when every message must be guaranteed.

---

## Backpressure — `!written` and `waitForDrain`

Two separate levels of backpressure exist when publishing:

**TCP level — `!written`**

`channel.publish()` returns `false` when the internal Node.js TCP write buffer is full. The OS cannot send bytes to the broker fast enough. `waitForDrain` pauses until the `drain` event fires, meaning the buffer has cleared:

```typescript
const written = channel.publish(...);
if (!written) await this.waitForDrain(channel);
```

**Broker level — `waitForConfirms()`**

Even after the bytes reach the broker over TCP, you may want to wait for RabbitMQ to confirm it has queued/persisted the message. `waitForConfirms()` handles this on a confirm channel.

```
publish()
  → written=false?  wait for TCP buffer to clear    (OS/Node.js level)
  → waitForConfirms()  wait for broker ack           (RabbitMQ level)
```

---

## Message Buffering on Disconnect

When `publish()` fails (connection is down), instead of throwing to the caller, the message is buffered:

```typescript
// caller does this
const result = await producer.publish(data); // hangs here, does not throw
```

Internally, the message is stored in `buffer[]` along with the `resolve` and `reject` functions of the caller's Promise:

```typescript
interface BufferedMessage {
  data: Record<string, any>;      // payload to republish
  options: PublishOptions;         // original publish options
  confirm: boolean;                // was it publish() or publishWithConfirm()?
  resolve: (value: boolean) => void; // unblocks the caller on success
  reject: (err: Error) => void;      // fails the caller if retry also fails
}
```

On reconnect, `flushBuffer()` replays every buffered message in order. When each succeeds, `msg.resolve(true)` is called — the original caller unblocks and gets their result as if nothing happened.

`maxBufferSize` prevents unbounded memory growth. If the buffer is full, the message is rejected immediately.

---

## onReconnect — Registered Once

`onReconnect` in the producer is registered exactly once in the constructor:

```typescript
rabbitInstance.onReconnect(async () => await this.flushBuffer());
```

Previously it was registered inside `bufferMessage`, which meant every buffered message added a new callback. With 10 buffered messages, `flushBuffer` would be called 10 times on reconnect — causing 10x duplicate message delivery. Registering once in the constructor eliminates this entirely.

---

## Consumer — Multi-Worker

Each worker is a separate channel with its own `prefetch` setting. Multiple workers on the same queue means parallel message processing:

```
Queue
  ├── Worker 0 (Channel 1, prefetch=1) → handler()
  ├── Worker 1 (Channel 2, prefetch=1) → handler()
  └── Worker 2 (Channel 3, prefetch=1) → handler()
```

`prefetchCount` controls how many unacknowledged messages each worker holds at a time. Keep it low (1–5) for fair distribution.

Workers re-establish themselves after reconnection via `onReconnect`.

---

## Retry with Exponential Backoff

Failed messages are requeued with a per-message TTL that doubles on each retry:

```
1st retry → 1s delay
2nd retry → 2s delay
3rd retry → 4s delay
after retryLimit → nack without requeue → DLX
```

Formula: `min(1000 * 2^retryCount, 30000)` — capped at 30 seconds.

Retry count is tracked via the `x-retry-count` header on the message itself.

---

## Serialization — JSON vs MessagePack vs Identity

### What is serialization?

RabbitMQ moves bytes — it has no concept of objects, strings, or numbers. Before a message can be sent over the network it must be **serialized** (object → bytes) and after receiving it must be **deserialized** (bytes → object).

```
{ orderId: "ORD-1" }  →  serialize  →  [bytes]  →  RabbitMQ  →  [bytes]  →  deserialize  →  { orderId: "ORD-1" }
```

### The three serializers

**`JsonSerializer` — `application/json` (default)**

Converts object to a UTF-8 string of bytes. Human readable.

```
{ orderId: "ORD-1", amount: 99.99 }
→ '{"orderId":"ORD-1","amount":99.99}'
→ [123, 34, 111, 114, 100, 101, 114, 73, 100, ...]
```

**`MsgpackSerializer` — `application/msgpack`**

Binary format. Same data as JSON but encoded as compact binary — 2-3x smaller, 2-3x faster to parse.

```
{ orderId: "ORD-1", amount: 99.99 }
→ [130, 167, 111, 114, 100, 101, 114, 73, 100, ...]  (binary, not readable)
```

**`IdentitySerializer` — `application/octet-stream`**

Raw bytes passthrough. No serialization — you pass a `Buffer`, we send it as-is. For when you handle encoding yourself (protobuf, avro, images, PDFs etc).

```typescript
const encoded = OrderProto.encode({ orderId: "ORD-1" }).finish();
await producer.publish(encoded); // sent exactly as-is
```

### Why JSON is the default despite being slower

**1. Human readability matters in production**

When something breaks at 3am, you open RabbitMQ management UI and read the queued message directly. JSON shows `{"orderId":"ORD-1"}`. MessagePack shows `[130, 167, 111, 114...]`. Debugging binary is painful.

**2. The performance difference rarely matters**

Most systems don't publish thousands of messages per second. At 100 messages/sec the difference between JSON and msgpack is microseconds — irrelevant. You'd optimize database queries long before message serialization becomes a bottleneck.

**3. JSON is universal**

Every language, every framework, every tool speaks JSON natively. MessagePack needs a library on both producer and consumer. In a polyglot microservices environment (Node, Python, Go, Java) JSON just works everywhere.

**4. Tooling is built around JSON**

RabbitMQ management UI, logging pipelines, monitoring dashboards, message inspectors — all built around JSON. MessagePack messages appear as garbage bytes in all of these.

**5. Network is rarely the bottleneck**

In most architectures the bottleneck is database queries, business logic, or external API calls — not message payload size. Optimizing serialization is premature optimization in 99% of cases.

### When MessagePack actually makes sense

- Publishing millions of messages per second
- Large payloads (kilobytes per message)
- You control both producer and consumer (same team, same codebase)
- Network bandwidth is genuinely constrained (IoT, mobile, edge)

### contentType auto-detection

Producer sets `contentType` on every message from the serializer:

```
message.properties.contentType = "application/msgpack"
```

Consumer reads it and picks the correct deserializer automatically — even if the consumer was configured with a different default. The message carries its own format information so producer and consumer don't need to be manually kept in sync.

```typescript
private deserialize(msg: amqp.ConsumeMessage): T {
  const contentType = msg.properties.contentType;
  const serializer = serializerRegistry[contentType] ?? this.serializer;
  return serializer.deserialize(msg.content) as T;
}
```

### Why protobuf is not a built-in serializer

Protobuf requires a shared `.proto` schema file on both producer and consumer. The library can't manage schemas — that's the user's responsibility. So protobuf is supported via `IdentitySerializer` — user encodes/decodes themselves and passes raw `Buffer` to us.

```typescript
// user handles protobuf encoding
const buf = OrderProto.encode({ orderId: "ORD-1" }).finish();
const producer = new RabbitProducer(handler, "orders", "order.created", {
  serializer: new IdentitySerializer()
});
await producer.publish(Buffer.from(buf));
```
