# Future Implementation Roadmap

This document tracks planned features, their design decisions, and implementation notes.

---

## 1. Connection Pool in Producer/Consumer

**Status:** Not implemented — manual pool usage only  
**Effort:** Medium  

### Current limitation
Producer and consumer accept `IRabbitConnection` which `RabbitSingleConnectionHandler` implements. `RabbitConnectionPoolHandler` does not implement `IRabbitConnection` so you have to manually acquire a connection from the pool and pass it:

```typescript
const conn = pool.acquire(); // manual
const producer = new RabbitProducer(conn, "orders", "order.created");
```

This ties the producer to one specific connection forever — defeating the purpose of the pool.

### Why it's non-trivial
Producer stores `this.channel` on a specific connection. If `rabbitConnection` returns a different connection each time (round-robin), the stored channel becomes invalid on the next call since it was created on a different connection.

### Planned approach — ChannelPool per connection
Pool maintains one channel per connection internally. Producer asks the pool for a channel, not a connection. The pool handles which connection to use:

```typescript
const pool = new RabbitConnectionPoolHandler(url, 5);
const producer = new RabbitProducer(pool, "orders", "order.created");
// pool internally round-robins channels across its connections
```

This requires:
- `RabbitConnectionPoolHandler` to implement `IRabbitConnection`
- Internal `Map<RabbitSingleConnectionHandler, amqp.Channel>` — one channel per connection
- `rabbitConnection` getter returns the connection whose channel is next in rotation
- On reconnect, invalidate only the channel for that specific connection

---

## 2. Message Serialization Options

**Status:** JSON only  
**Effort:** Low  

### Planned approach
Add a `MessageSerializer` interface. Ship JSON as default. User plugs in msgpack, protobuf, avro etc:

```typescript
interface MessageSerializer {
  serialize(data: unknown): Buffer;
  deserialize(buffer: Buffer): unknown;
  contentType: string;
}

// usage
const producer = new RabbitProducer(handler, "orders", "order.created", {
  serializer: new MsgpackSerializer()
});
```

Built-in serializers to ship:
- `JsonSerializer` — default, current behavior
- `MsgpackSerializer` — smaller payload, faster for high throughput
- `IdentitySerializer` — raw Buffer passthrough for binary data

Consumer automatically uses the serializer matching the message `contentType` header, or falls back to JSON.

---

## 3. Headers Exchange Consumer Routing

**Status:** Not implemented  
**Effort:** Low  

### Current limitation
Consumer ignores message headers for routing. Headers exchange queues are set up correctly via `createQueue` with header args, but the consumer has no way to filter by headers.

### Planned approach
Add `matchHeaders` to `ConsumeOptions`:

```typescript
await consumer.consume("alerts.critical", async (data) => {}, {
  matchHeaders: {
    "x-match": "all",
    type: "error",
    severity: "critical",
  }
});
```

Internally, before calling the handler, check `msg.properties.headers` against `matchHeaders`. If no match, nack without requeue (let RabbitMQ route it elsewhere).

---

## 4. Priority Queue Support

**Status:** Partially implemented — `priority` exists in `PublishOptions` but no queue-level setup  
**Effort:** Low  

### Planned approach
Expose `maxPriority` in `createQueue` and document the full flow:

```typescript
// exchange setup
await exchange.createQueue("orders.priority", "order.#", {}, {
  "x-max-priority": 10
});

// producer — higher priority processed first
await producer.publish({ orderId: "ORD-VIP" }, { priority: 9 });
await producer.publish({ orderId: "ORD-STD" }, { priority: 1 });
```

`QueueArguments` already has `x-max-priority` — just needs documentation and a dedicated `createPriorityQueue` helper method on `RabbitMqQueueExchange`.

---

## 5. Circuit Breaker

**Status:** Not implemented — reconnect retries indefinitely until `maxReconnectAttempts`  
**Effort:** Medium  

### Current limitation
If the broker is flaky (connects then drops repeatedly), we keep retrying with no backoff beyond the fixed `reconnectInterval`. This can cause thundering herd on broker restart.

### Planned approach
Three-state circuit breaker inside `RabbitSingleConnectionHandler`:

```
CLOSED    → normal operation, failures tracked
OPEN      → threshold exceeded, stop trying, wait resetTimeout
HALF_OPEN → after resetTimeout, try one connection, if success → CLOSED, if fail → OPEN
```

```typescript
const handler = new RabbitSingleConnectionHandler(url, {
  circuitBreaker: {
    threshold: 5,        // consecutive failures before opening
    resetTimeout: 30000, // ms to wait before trying again
  }
});

handler.on("circuit.open", () => logger.error("Circuit opened"));
handler.on("circuit.closed", () => logger.info("Circuit closed"));
```

Pairs with the metrics/telemetry hooks (item 6).

---

## 6. Metrics / Telemetry Hooks

**Status:** Log-based observability only (JSON logs parseable by Loki, CloudWatch, Datadog)  
**Effort:** Low  

### Current state
Structured JSON logs in production cover most observability needs. Tools like Loki + Grafana, CloudWatch Logs Insights, and Datadog Logs can query and alert on them directly without any code changes.

### Planned approach for real-time metrics
Extend `EventEmitter` on each class. User hooks in whatever metrics backend they want — zero coupling to Prometheus, Datadog, StatsD etc:

```typescript
// connection
handler.on("reconnecting", ({ attempt, maxAttempts }) => {});
handler.on("reconnected", ({ attempt }) => {});
handler.on("disconnected", () => {});
handler.on("circuit.open", () => {});
handler.on("circuit.closed", () => {});

// producer
producer.on("message.published", ({ exchange, routingKey }) => {});
producer.on("message.confirmed", ({ exchange, routingKey }) => {});
producer.on("message.buffered", ({ buffered, max, exchange }) => {});
producer.on("message.flushed", ({ count, exchange }) => {});
producer.on("buffer.full", ({ exchange }) => {});

// consumer
consumer.on("message.received", ({ queue }) => {});
consumer.on("message.acked", ({ queue }) => {});
consumer.on("message.nacked", ({ queue }) => {});
consumer.on("message.retrying", ({ queue, attempt, delay }) => {});
consumer.on("message.skipped", ({ queue, retryLimit }) => {});
```

Example Prometheus integration:

```typescript
const reconnectCounter = new Counter({ name: "rabbitmq_reconnects_total" });
const bufferGauge = new Gauge({ name: "rabbitmq_buffer_size" });

handler.on("reconnecting", () => reconnectCounter.inc());
producer.on("message.buffered", ({ buffered }) => bufferGauge.set(buffered));
```

---

## 7. RPC Pattern (Request/Reply)

**Status:** Not implemented  
**Effort:** Medium  

### Planned approach
New `RabbitRPC<TRequest, TResponse>` class built on top of producer and consumer:

```typescript
const rpc = new RabbitRPC<GetUserRequest, GetUserResponse>(handler, "rpc.exchange");

// caller side
const response = await rpc.call("user.get", { userId: "U1" }, { timeout: 5000 });
console.log(response.name); // fully typed

// handler side
await rpc.handle("user.get", async (data) => {
  return { name: "John", email: "john@example.com" };
});
```

Internals:
- Caller creates a temporary exclusive auto-delete reply queue per call
- Sets `correlationId` (UUID) and `replyTo` (reply queue name) on the message
- Waits for a message on the reply queue matching the `correlationId`
- Handler reads `replyTo` and `correlationId` from incoming message, publishes response back
- Timeout rejects the promise if no response within the window

---

## 8. Middleware / Plugin System

**Status:** Not implemented  
**Effort:** High — architectural change  

### Planned approach
Pipeline pattern on producer and consumer. Middleware runs in order, each calls `next()` to continue:

```typescript
// producer middleware
producer.use(async (context, next) => {
  context.options.headers = {
    ...context.options.headers,
    "x-trace-id": generateTraceId(),
    "x-source-service": "order-service",
  };
  await next();
});

// consumer middleware
consumer.use(async (context, next) => {
  const start = Date.now();
  await next();
  metrics.timing("message.processing", Date.now() - start, { queue: context.queue });
});

// error handling middleware
consumer.use(async (context, next) => {
  try {
    await next();
  } catch (err) {
    logger.error("Unhandled message error", { err, queue: context.queue });
    throw err; // re-throw to trigger dlx/retry
  }
});
```

Context objects:
```typescript
interface ProducerContext<T> {
  data: T;
  options: PublishOptions;
  exchange: string;
  routingKey: string;
}

interface ConsumerContext<T> {
  data: T;
  msg: amqp.ConsumeMessage;
  queue: string;
}
```

---

## Implementation Order

| # | Feature | Effort | Breaking | Priority |
|---|---------|--------|----------|----------|
| 1 | Serialization options | Low | No | High |
| 2 | Priority queue helper | Low | No | Medium |
| 3 | Headers consumer routing | Low | No | Medium |
| 4 | Metrics/telemetry hooks | Low | No | High |
| 5 | Circuit breaker | Medium | No | High |
| 6 | Connection pool in producer | Medium | No | Medium |
| 7 | RPC pattern | Medium | No | Low |
| 8 | Middleware system | High | No | Low |

All planned features are **additive and non-breaking** — existing code will continue to work unchanged.
