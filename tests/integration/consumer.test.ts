import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { RabbitMqQueueExchange } from "../../src/exchange/exchange.js";
import { RabbitProducer } from "../../src/producer/producer.js";
import { RabbitConsumer } from "../../src/consumer/consumer.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;
let handler: RabbitSingleConnectionHandler;

class TestExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "test.exchange", "direct");
  }
  async setup(queueName: string, routingKey: string) {
    await this.createExchange();
    await this.createQueue(queueName, routingKey);
  }
  async setupWithDlx(mainQueue: string, dlxQueue: string, routingKey: string) {
    await this.createExchange();
    await this.createQueue(dlxQueue, `${routingKey}.failed`);
    await this.createQueue(mainQueue, routingKey, { durable: true }, {
      "x-dead-letter-exchange": "test.exchange",
      "x-dead-letter-routing-key": `${routingKey}.failed`,
    });
  }
}

beforeAll(async () => {
  container = await startRabbitContainer();
  amqpUrl = getAmqpUrl(container);
});

afterAll(async () => {
  await stopRabbitContainer(container);
});

beforeEach(async () => {
  handler = new RabbitSingleConnectionHandler(amqpUrl);
  await handler.ConnectToService();
});

afterEach(async () => {
  await handler.gracefulShutdown();
});

describe("RabbitConsumer", () => {
  it("receives message with correct payload intact", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setup("c.payload", "c.payload.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.payload.key");
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("c.payload", async (data) => resolve(data));
    });

    await producer.publish({ id: "P1", nested: { value: 42 }, arr: [1, 2, 3] });

    const msg = await received;
    expect(msg.id).toBe("P1");
    expect(msg.nested.value).toBe(42);
    expect(msg.arr).toEqual([1, 2, 3]);
  });

  it("prefetch=1 ensures at most 1 unacked message in flight", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setup("c.prefetch", "c.prefetch.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.prefetch.key");
    const consumer = new RabbitConsumer(handler);

    let concurrent = 0;
    let maxConcurrent = 0;

    await consumer.consume("c.prefetch", async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 100));
      concurrent--;
    }, { prefetchCount: 1 });

    for (let i = 0; i < 5; i++) await producer.publish({ i });

    await new Promise((r) => setTimeout(r, 1000));

    // with prefetch=1, only 1 message in flight at a time
    expect(maxConcurrent).toBe(1);
  });

  it("processes all messages and each exactly once", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setup("c.all", "c.all.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.all.key");
    const consumer = new RabbitConsumer(handler);
    const received = new Set<string>();

    await consumer.consume("c.all", async (data) => {
      received.add(data.id);
    });

    const ids = Array.from({ length: 10 }, (_, i) => `msg-${i}`);
    for (const id of ids) await producer.publish({ id });

    await new Promise((r) => setTimeout(r, 1000));

    expect(received.size).toBe(10);
    for (const id of ids) expect(received.has(id)).toBe(true);
  });

  it("retry with backoff — retries exact times with increasing delays", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setup("c.retry", "c.retry.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.retry.key");
    const consumer = new RabbitConsumer(handler);

    const attempts: number[] = [];
    const timestamps: number[] = [];

    const done = new Promise<void>((resolve) => {
      consumer.consume("c.retry", async () => {
        attempts.push(attempts.length + 1);
        timestamps.push(Date.now());
        if (attempts.length < 3) throw new Error("fail");
        resolve();
      }, { retryLimit: 3, dlx: false });
    });

    await producer.publish({ id: "retry-msg" });
    await done;

    expect(attempts).toHaveLength(3);

    const delay1 = timestamps[1] - timestamps[0];
    const delay2 = timestamps[2] - timestamps[1];
    expect(delay1).toBeGreaterThanOrEqual(900);
    expect(delay2).toBeGreaterThanOrEqual(1900);
    expect(delay2).toBeGreaterThan(delay1);
  }, 15000);

  it("after exhausting retries, acks and continues to next message", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setup("c.exhaust", "c.exhaust.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.exhaust.key");
    const consumer = new RabbitConsumer(handler);
    let failAttempts = 0;

    const secondReceived = new Promise<string>((resolve) => {
      consumer.consume("c.exhaust", async (data) => {
        if (data.id === "fail-msg") {
          failAttempts++;
          throw new Error("always fails");
        }
        if (data.id === "next-msg") resolve(data.id);
      }, { retryLimit: 2, dlx: false });
    });

    await producer.publish({ id: "fail-msg" });
    await new Promise((r) => setTimeout(r, 200));
    await producer.publish({ id: "next-msg" });

    const result = await secondReceived;
    expect(result).toBe("next-msg");
    expect(failAttempts).toBe(3);
  }, 15000);

  it("dlx: true — failed message nacked to DLX immediately, not retried", async () => {
    const exchange = new TestExchange(handler);
    await exchange.setupWithDlx("c.dlx.main", "c.dlx.failed", "c.dlx.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.dlx.key");
    const mainConsumer = new RabbitConsumer(handler);
    const dlxConsumer = new RabbitConsumer(handler);
    let mainAttempts = 0;

    const dlxReceived = new Promise<Record<string, any>>((resolve) => {
      dlxConsumer.consume("c.dlx.failed", async (data) => resolve(data));
    });

    await mainConsumer.consume("c.dlx.main", async (data) => {
      mainAttempts++;
      throw new Error("always fails");
    }, { dlx: true });

    await producer.publish({ id: "dlx-msg", payload: "test" });

    const dlxMsg = await dlxReceived;
    expect(dlxMsg.id).toBe("dlx-msg");
    expect(dlxMsg.payload).toBe("test");
    expect(mainAttempts).toBe(1);
  });

  it("consumer re-establishes after connection drop and continues receiving", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await handler2.ConnectToService();

    const exchange = new TestExchange(handler);
    await exchange.setup("c.reconnect", "c.reconnect.key");

    const producer = new RabbitProducer(handler, "test.exchange", "c.reconnect.key");
    const consumer = new RabbitConsumer(handler2);
    const received: string[] = [];

    await consumer.consume("c.reconnect", async (data) => {
      received.push(data.id);
    });

    await producer.publish({ id: "before-drop" });
    await new Promise((r) => setTimeout(r, 300));

    await handler2.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 1000));

    await producer.publish({ id: "after-reconnect" });
    await new Promise((r) => setTimeout(r, 500));

    expect(received).toContain("before-drop");
    expect(received).toContain("after-reconnect");

    await handler2.gracefulShutdown();
  });

  it("throws when no active connection on consume", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, { maxReconnectAttempts: 0 });
    await handler2.ConnectToService();
    await handler2.gracefulShutdown();

    const consumer = new RabbitConsumer(handler2);
    await expect(consumer.consume("some.queue", async () => {})).rejects.toThrow("No active connection");
  });
});
