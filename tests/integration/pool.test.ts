import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitConnectionPoolHandler } from "../../src/connection/pool.js";
import { RabbitMqQueueExchange } from "../../src/exchange/exchange.js";
import { RabbitProducer } from "../../src/producer/producer.js";
import { RabbitConsumer } from "../../src/consumer/consumer.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;

class TestExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "pool.exchange", "direct");
  }
  async setup(queueName: string, routingKey: string) {
    await this.createExchange();
    await this.createQueue(queueName, routingKey);
  }
}

beforeAll(async () => {
  container = await startRabbitContainer();
  amqpUrl = getAmqpUrl(container);
});

afterAll(async () => {
  await stopRabbitContainer(container);
});

describe("RabbitConnectionPoolHandler", () => {
  it("initializes pool and all connections are active", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    // rabbitConnection getter should return a live connection
    expect(pool.rabbitConnection).not.toBeNull();

    await pool.gracefulShutdown();
  });

  it("getChannel round-robins across connections — each connection gets its own channel", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    // get 3 channels — should be created on different connections
    const ch1 = await pool.getChannel();
    const ch2 = await pool.getChannel();
    const ch3 = await pool.getChannel();

    // all channels should be valid
    expect(ch1).toBeDefined();
    expect(ch2).toBeDefined();
    expect(ch3).toBeDefined();

    // 4th call wraps around — returns ch1 again (same channel reused for conn1)
    const ch4 = await pool.getChannel();
    expect(ch4).toBe(ch1);

    await pool.gracefulShutdown();
  });

  it("producer uses pool — publishes distributed across connections", async () => {
    const setupHandler = new RabbitSingleConnectionHandler(amqpUrl);
    await setupHandler.ConnectToService();

    const exchange = new TestExchange(setupHandler);
    await exchange.setup("pool.orders", "pool.order.created");

    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    const producer = new RabbitProducer(pool, "pool.exchange", "pool.order.created");
    const consumer = new RabbitConsumer(setupHandler);
    const received: string[] = [];

    await consumer.consume("pool.orders", async (data) => {
      received.push(data.id);
    });

    // publish 9 messages — 3 per connection via round-robin
    for (let i = 0; i < 9; i++) {
      await producer.publish({ id: `msg-${i}` });
    }

    await new Promise((r) => setTimeout(r, 500));

    expect(received).toHaveLength(9);
    for (let i = 0; i < 9; i++) expect(received).toContain(`msg-${i}`);

    await pool.gracefulShutdown();
    await setupHandler.gracefulShutdown();
  });

  it("consumer uses pool — receives messages correctly", async () => {
    const setupHandler = new RabbitSingleConnectionHandler(amqpUrl);
    await setupHandler.ConnectToService();

    const exchange = new TestExchange(setupHandler);
    await exchange.setup("pool.consumer.queue", "pool.consumer.key");

    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    const producer = new RabbitProducer(setupHandler, "pool.exchange", "pool.consumer.key");
    const consumer = new RabbitConsumer(pool);

    const received = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("pool.consumer.queue", async (data) => resolve(data));
    });

    await producer.publish({ orderId: "ORD-POOL-1" });

    const msg = await received;
    expect(msg.orderId).toBe("ORD-POOL-1");

    await setupHandler.gracefulShutdown();
    await pool.gracefulShutdown();
  });

  it("channel is reused for same connection on subsequent getChannel calls", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 2);
    await pool.ConnectToService();

    // first round — creates channels
    const ch1 = await pool.getChannel();
    const ch2 = await pool.getChannel();

    // second round — reuses same channels
    const ch1Again = await pool.getChannel();
    const ch2Again = await pool.getChannel();

    expect(ch1Again).toBe(ch1);
    expect(ch2Again).toBe(ch2);

    await pool.gracefulShutdown();
  });

  it("invalidates channel for dropped connection and recreates on reconnect", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 2, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await pool.ConnectToService();

    const setupHandler = new RabbitSingleConnectionHandler(amqpUrl);
    await setupHandler.ConnectToService();

    const exchange = new TestExchange(setupHandler);
    await exchange.setup("pool.reconnect.queue", "pool.reconnect.key");

    const producer = new RabbitProducer(pool, "pool.exchange", "pool.reconnect.key");
    const consumer = new RabbitConsumer(setupHandler);
    const received: string[] = [];

    await consumer.consume("pool.reconnect.queue", async (data) => {
      received.push(data.id);
    });

    await producer.publish({ id: "before-drop" });
    await new Promise((r) => setTimeout(r, 300));

    // force drop one connection in the pool
    const conn = pool.rabbitConnection;
    await conn!.close();
    await new Promise((r) => setTimeout(r, 1000)); // wait for reconnect

    await producer.publish({ id: "after-reconnect" });
    await new Promise((r) => setTimeout(r, 500));

    expect(received).toContain("before-drop");
    expect(received).toContain("after-reconnect");

    await pool.gracefulShutdown();
    await setupHandler.gracefulShutdown();
  });

  it("gracefulShutdown clears all channels and connections", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    // create some channels
    await pool.getChannel();
    await pool.getChannel();

    await pool.gracefulShutdown();

    // after shutdown, rabbitConnection should be null
    expect(pool.rabbitConnection).toBeNull();
  });
});
