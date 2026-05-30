# rabbit-axon

Production-ready class-based TypeScript RabbitMQ wrapper — auto-reconnect, dead-letter exchange, publisher confirms, exponential backoff retries, message buffering, connection pooling, and structured Winston logging.

---

## Installation

```bash
pnpm add rabbit-axon
```

---

## Project Structure

```
src/
├── connection/
│   ├── single.ts       — Single connection handler with auto-reconnect
│   └── pool.ts         — Connection pool handler (acquire)
├── channel/
│   └── manager.ts      — Static channel utility (create, close)
├── exchange/
│   └── exchange.ts     — Abstract exchange/queue setup
├── producer/
│   └── producer.ts     — Publish, confirm publish, message buffering
├── consumer/
│   └── consumer.ts     — Consume, DLX routing, retry backoff
├── logger/
│   └── logger.ts       — Winston-based structured logger
└── types.ts            — All interfaces and type aliases
```

---

## Quick Start

```typescript
import {
  RabbitSingleConnectionHandler,
  RabbitMqQueueExchange,
  RabbitProducer,
  RabbitConsumer,
} from "rabbit-axon";

// 1. Connect
const handler = new RabbitSingleConnectionHandler("amqp://localhost");
await handler.ConnectToService();

// 2. Setup exchange + queue
class OrderExchange extends RabbitMqQueueExchange {
  constructor() {
    super(handler, "orders", "topic");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("orders.created", "order.created.#");
  }
}
const exchange = new OrderExchange();
await exchange.setup();

// 3. Produce
const producer = new RabbitProducer(handler, "orders", "order.created.us");
await producer.publish({ orderId: "ORD-1", region: "us" });

// 4. Consume
const consumer = new RabbitConsumer(handler);
await consumer.consume("orders.created", async (data) => {
  console.log("Order received:", data.orderId);
});

// 5. Shutdown
process.on("SIGTERM", async () => {
  await handler.gracefulShutdown();
});
```

---

## Connection

### RabbitSingleConnectionHandler

Single AMQP connection with auto-reconnect.

```typescript
import { RabbitSingleConnectionHandler } from "rabbit-axon";

const handler = new RabbitSingleConnectionHandler("amqp://user:pass@localhost:5672", {
  heartbeat: 30,
  reconnectInterval: 3000,
  maxReconnectAttempts: 15,
  channelMax: 100,
});

await handler.ConnectToService();

// Register reconnect callback
handler.onReconnect(async () => {
  console.log("Reconnected — re-setup if needed");
});

// Graceful shutdown
await handler.gracefulShutdown();
```

### RabbitConnectionPoolHandler

Multiple connections for high-throughput scenarios. Each connection in the pool has its own auto-reconnect.

```typescript
import { RabbitConnectionPoolHandler } from "rabbit-axon";

const pool = new RabbitConnectionPoolHandler("amqp://localhost", 5, {
  heartbeat: 30,
});

await pool.ConnectToService();

// Acquire an active connection (round-robin, skips dropped connections)
const conn = pool.acquire();

// Pass to exchange, producer, or consumer
const producer = new RabbitProducer(conn, "orders", "order.created");

await pool.gracefulShutdown();
```

---

## Logger

Every class creates a default Winston logger internally. Replace it with your own:

```typescript
import { RabbitLogger, RabbitSingleConnectionHandler } from "rabbit-axon";
import winston from "winston";

const myWinstonLogger = winston.createLogger({
  level: "info",
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "rabbit.log" }),
  ],
});

const logger = new RabbitLogger(myWinstonLogger);

const handler = new RabbitSingleConnectionHandler("amqp://localhost");
handler.addLogger(logger);
```

Default behavior (no custom logger):
- `NODE_ENV=production` → JSON logs (one line per entry, parseable by Loki/CloudWatch/Datadog/ELK)
- `NODE_ENV=development` → colored human-readable logs

---

## Exchange Types

### fanout — Broadcast to all queues

```typescript
class BroadcastExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "events.broadcast", "fanout");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("service-a.events", "");
    await this.createQueue("service-b.events", "");
  }
}
```

### direct — Exact routing key match

```typescript
class LogExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "logs", "direct");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("logs.errors", "error");
    await this.createQueue("logs.warnings", "warning");
  }
}

// Only "logs.errors" receives this
const producer = new RabbitProducer(handler, "logs", "error");
await producer.publish({ msg: "DB connection failed" });
```

### topic — Pattern routing key

```typescript
class OrderExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "orders", "topic");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("orders.all", "order.#");
    await this.createQueue("orders.created", "order.created.*");
    await this.createQueue("orders.us", "order.*.us");
  }
}

// Matches "orders.all", "orders.created", "orders.us"
const producer = new RabbitProducer(handler, "orders", "order.created.us");
await producer.publish({ orderId: "ORD-1" });
```

### headers — Route by message headers

```typescript
class AlertExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "alerts", "headers");
  }
  async setup() {
    await this.createExchange();
    // Both headers must match
    await this.createQueue("alerts.critical", "", { durable: true }, undefined, {
      "x-match": "all",
      type: "error",
      severity: "critical",
    });
    // Any header matches
    await this.createQueue("alerts.any", "", { durable: true }, undefined, {
      "x-match": "any",
      type: "error",
      severity: "critical",
    });
  }
}

const producer = new RabbitProducer(handler, "alerts", "");
await producer.publish({ message: "Disk full" }, {
  headers: { type: "error", severity: "critical" },
});
```

---

## Dead Letter Exchange (DLX)

```typescript
class OrderExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "orders", "topic");
  }
  async setup() {
    await this.createExchange();

    // DLX queue to catch failed messages
    await this.createQueue("orders.failed", "order.failed.#");

    // Main queue with DLX configured
    await this.createDeadLetterQueue(
      "orders.processing",  // queue name
      "orders",             // DLX exchange
      "order.failed.all",   // DLX routing key
      30000                 // TTL: 30s before expiry also routes to DLX
    );
  }
}

// Consumer with dlx: true — failed messages nack'd straight to DLX
const consumer = new RabbitConsumer(handler);
await consumer.consume("orders.processing", async (data) => {
  throw new Error("processing failed"); // → goes to orders.failed queue
}, { dlx: true });
```

---

## Producer

### Basic publish

```typescript
const producer = new RabbitProducer(handler, "orders", "order.created");

await producer.publish({ orderId: "ORD-1" }, {
  persistent: true,
  priority: 5,
  messageId: "msg-001",
  headers: { "x-source": "order-service" },
});
```

### TypeScript generics

```typescript
interface OrderPayload {
  orderId: string;
  amount: number;
  region: "us" | "eu";
}

const producer = new RabbitProducer<OrderPayload>(handler, "orders", "order.created");
await producer.publish({ orderId: "ORD-1", amount: 99.99, region: "us" }); // fully type checked
```

### Publish with broker confirm

```typescript
const confirmed = await producer.publishWithConfirm({ orderId: "ORD-1" }, {
  persistent: true,
  expiration: "60000",
});

if (!confirmed) console.error("Broker rejected the message");
```

### Message buffering on disconnect

When the connection drops, `publish` does not throw — it buffers the message and resolves once the connection is restored:

```typescript
// hangs until reconnected, then resolves automatically
const result = await producer.publish({ orderId: "ORD-2" });

console.log(producer.getBufferSize()); // number of pending buffered messages
```

---

## Consumer

### Basic consume

```typescript
const consumer = new RabbitConsumer(handler);

await consumer.consume("orders.created", async (data, msg) => {
  console.log("Processing:", data.orderId);
}, { prefetchCount: 1 });
```

### TypeScript generics

```typescript
interface OrderPayload {
  orderId: string;
  amount: number;
}

const consumer = new RabbitConsumer<OrderPayload>(handler);
await consumer.consume("orders.created", async (data) => {
  console.log(data.orderId); // string — fully typed
  console.log(data.amount);  // number — fully typed
});
```

### Retry with exponential backoff (no DLX)

When `dlx: false` (default), failed messages are retried in-process with exponential backoff. After `retryLimit`, the message is acked and skipped:

```typescript
await consumer.consume("orders.created", async (data) => {
  await processOrder(data);
}, {
  retryLimit: 3,
  dlx: false,
});
```

| Attempt | Delay |
|---------|-------|
| 1st retry | 1s |
| 2nd retry | 2s |
| 3rd retry | 4s |
| After limit | ack + skip |

### DLX routing on failure

When `dlx: true`, any failure immediately nacks the message — RabbitMQ routes it to the configured DLX:

```typescript
await consumer.consume("orders.processing", async (data) => {
  await processOrder(data);
}, { dlx: true });
```

---

## Custom Logger per class

```typescript
const logger = new RabbitLogger(myWinstonLogger);

handler.addLogger(logger);
pool.addLogger(logger);       // propagates to all connections in pool
exchange.addLogger(logger);
producer.addLogger(logger);
consumer.addLogger(logger);
```

---

## Graceful Shutdown

```typescript
process.on("SIGTERM", async () => {
  await handler.gracefulShutdown();
  process.exit(0);
});
```

---

## Production Considerations

- `persistent: true` by default — messages survive broker restarts
- `noAck: false` — all messages must be explicitly acknowledged
- After `maxReconnectAttempts`, `process.exitCode = 1` — let PM2/K8s/ECS restart the process
- Keep `prefetchCount` low (1–5) for fair distribution
- Use `dlx: true` + a dead letter queue for guaranteed message capture on failure
- Monitor DLQ depth — a growing DLQ means your handler is consistently failing
- Use `publishWithConfirm` for critical messages where loss is unacceptable
- Use `RabbitConnectionPoolHandler` for high-throughput services needing parallel TCP connections
- Scale consumers by deploying multiple instances/containers — each gets its own connection and channel

---

## API Reference

### Connection Options

Passed as the second argument to `RabbitSingleConnectionHandler` and `RabbitConnectionPoolHandler`.

```typescript
new RabbitSingleConnectionHandler(url, options)
new RabbitConnectionPoolHandler(url, poolSize, options)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `heartbeat` | `number` | `60` | Heartbeat interval in seconds. Detects dead connections. |
| `reconnectInterval` | `number` | `5000` | Milliseconds to wait between reconnect attempts. |
| `maxReconnectAttempts` | `number` | `10` | Max reconnect attempts before setting `process.exitCode = 1`. |
| `frameMax` | `number` | `0` | Max frame size in bytes. `0` = no limit. |
| `channelMax` | `number` | `0` | Max channels per connection. `0` = no limit. |

```typescript
const handler = new RabbitSingleConnectionHandler("amqp://localhost", {
  heartbeat: 30,
  reconnectInterval: 3000,
  maxReconnectAttempts: 15,
  channelMax: 100,
});
```

---

### Exchange Options

Passed as the fourth argument to `RabbitMqQueueExchange` constructor.

```typescript
super(handler, "exchange-name", "topic", exchangeOptions)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `durable` | `boolean` | `true` | Exchange survives broker restart. |
| `autoDelete` | `boolean` | `false` | Exchange deleted when last queue unbinds. |
| `internal` | `boolean` | `false` | Exchange only receives messages from other exchanges, not producers. |
| `alternateExchange` | `string` | — | Exchange to route unroutable messages to. |
| `arguments` | `Record<string, any>` | — | Additional broker-specific arguments. |

```typescript
class MyExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "my.exchange", "topic", {
      durable: true,
      autoDelete: false,
      alternateExchange: "my.unrouted",
    });
  }
}
```

---

### Queue Arguments

Passed as the fourth argument to `createQueue` and `createDeadLetterQueue`.

```typescript
await this.createQueue(queueName, bindKey, queueOptions, queueArguments)
```

| Argument | Type | Description |
|----------|------|-------------|
| `x-dead-letter-exchange` | `string` | Exchange to route rejected/expired messages to. |
| `x-dead-letter-routing-key` | `string` | Routing key to use when forwarding to DLX. |
| `x-message-ttl` | `number` | Milliseconds before a message expires and routes to DLX. |
| `x-max-length` | `number` | Max number of messages in the queue. Oldest dropped when exceeded. |
| `x-max-priority` | `number` | Enables priority queue. Value is the max priority level (1–255). |
| `x-queue-mode` | `"default" \| "lazy"` | `lazy` keeps messages on disk, reduces memory usage. |
| `x-expires` | `number` | Milliseconds before an unused queue is deleted. |
| `x-overflow` | `"drop-head" \| "reject-publish"` | Behaviour when `x-max-length` is exceeded. |

```typescript
await this.createQueue("orders.processing", "order.processing", { durable: true }, {
  "x-dead-letter-exchange": "orders",
  "x-dead-letter-routing-key": "order.failed",
  "x-message-ttl": 30000,
  "x-max-length": 10000,
  "x-overflow": "reject-publish",
});
```

---

### Publish Options

Passed as the second argument to `producer.publish()` and `producer.publishWithConfirm()`.

```typescript
await producer.publish(data, publishOptions)
await producer.publishWithConfirm(data, publishOptions)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `persistent` | `boolean` | `true` | Message survives broker restart. |
| `priority` | `number` | — | Message priority (requires `x-max-priority` on queue). |
| `expiration` | `string` | — | Milliseconds as string before message expires. |
| `correlationId` | `string` | — | Used to correlate RPC responses with requests. |
| `replyTo` | `string` | — | Queue name for RPC reply. |
| `messageId` | `string` | — | Application-level message identifier. |
| `timestamp` | `number` | `Date.now()` | Unix timestamp of when the message was created. |
| `contentType` | `string` | `"application/json"` | MIME type of the message body. |
| `headers` | `Record<string, any>` | — | Arbitrary message headers. Used for headers exchange routing. |

```typescript
await producer.publish({ orderId: "ORD-1" }, {
  persistent: true,
  priority: 5,
  expiration: "60000",
  messageId: "msg-001",
  headers: {
    "x-source": "order-service",
    "x-region": "us",
  },
});
```

---

### Consume Options

Passed as the third argument to `consumer.consume()`.

```typescript
await consumer.consume(queueName, handler, consumeOptions)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `prefetchCount` | `number` | `1` | Max unacknowledged messages held by this consumer at a time. |
| `retryLimit` | `number` | `3` | Max retry attempts before acking and skipping (only when `dlx: false`). |
| `dlx` | `boolean` | `false` | If `true`, failed messages are nacked immediately to DLX. If `false`, retried with exponential backoff. |

```typescript
// with DLX
await consumer.consume("orders.processing", async (data) => {
  await processOrder(data);
}, {
  prefetchCount: 1,
  dlx: true,
});

// with retry backoff
await consumer.consume("orders.created", async (data) => {
  await processOrder(data);
}, {
  prefetchCount: 5,
  retryLimit: 5,
  dlx: false,
});
```
