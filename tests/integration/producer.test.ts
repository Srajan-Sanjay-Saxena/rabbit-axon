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

class OrderExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "orders", "direct");
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

beforeEach(async () => {
  handler = new RabbitSingleConnectionHandler(amqpUrl);
  await handler.ConnectToService();
});

afterEach(async () => {
  await handler.gracefulShutdown();
});

describe("RabbitProducer", () => {
  it("publishes and message is actually received by consumer", async () => {
    const exchange = new OrderExchange(handler);
    await exchange.setup("orders.created", "order.created");

    const producer = new RabbitProducer(handler, "orders", "order.created");
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("orders.created", async (data) => resolve(data));
    });

    await producer.publish({ orderId: "ORD-1", amount: 99.99 });

    const msg = await received;
    expect(msg.orderId).toBe("ORD-1");
    expect(msg.amount).toBe(99.99);
  });

  it("publishWithConfirm gets broker ack and message is actually delivered", async () => {
    const exchange = new OrderExchange(handler);
    await exchange.setup("orders.confirmed", "order.confirmed");

    const producer = new RabbitProducer(handler, "orders", "order.confirmed");
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("orders.confirmed", async (data) => resolve(data));
    });

    const confirmed = await producer.publishWithConfirm({ orderId: "ORD-2" });
    expect(confirmed).toBe(true);

    const msg = await received;
    expect(msg.orderId).toBe("ORD-2");
  });

  it("reuses same channel across multiple publishes — no channel leak", async () => {
    const exchange = new OrderExchange(handler);
    await exchange.setup("orders.reuse", "order.reuse");

    const producer = new RabbitProducer(handler, "orders", "order.reuse");
    const consumer = new RabbitConsumer(handler);
    const received: string[] = [];

    await consumer.consume("orders.reuse", async (data) => {
      received.push(data.id);
    });

    for (let i = 0; i < 10; i++) await producer.publish({ id: `msg-${i}` });

    await new Promise((r) => setTimeout(r, 500));

    expect(received).toHaveLength(10);
    expect(producer.getBufferSize()).toBe(0);
  });

  it("buffers messages on connection drop, flushes after reconnect, caller unblocks", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await handler2.ConnectToService();

    const exchange = new OrderExchange(handler);
    await exchange.setup("orders.buffer", "order.buffer");

    const producer = new RabbitProducer(handler2, "orders", "order.buffer");
    const consumer = new RabbitConsumer(handler);
    const received: string[] = [];

    await consumer.consume("orders.buffer", async (data) => {
      received.push(data.id);
    });

    await handler2.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 50));

    const p1 = producer.publish({ id: "buf-1" });
    const p2 = producer.publish({ id: "buf-2" });
    const p3 = producer.publish({ id: "buf-3" });

    await new Promise((r) => setTimeout(r, 50));
    expect(producer.getBufferSize()).toBe(3);

    await Promise.all([p1, p2, p3]);
    expect(producer.getBufferSize()).toBe(0);

    await new Promise((r) => setTimeout(r, 500));
    expect(received).toHaveLength(3);

    await handler2.gracefulShutdown();
  });

  it("rejects immediately when buffer is full", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 0,
    });
    await handler2.ConnectToService();

    const producer = new RabbitProducer(handler2, "orders", "order.overflow", { maxBufferSize: 2 });

    await handler2.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 50));

    producer.publish({ id: 1 }).catch(() => {});
    producer.publish({ id: 2 }).catch(() => {});
    await new Promise((r) => setTimeout(r, 50));

    await expect(producer.publish({ id: 3 })).rejects.toThrow("Buffer full");
    expect(producer.getBufferSize()).toBe(2);
  });

  it("channels reset after reconnect — publishes succeed with fresh channel", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await handler2.ConnectToService();

    const exchange = new OrderExchange(handler);
    await exchange.setup("orders.reset", "order.reset");

    const producer = new RabbitProducer(handler2, "orders", "order.reset");
    const consumer = new RabbitConsumer(handler);
    const received: string[] = [];

    await consumer.consume("orders.reset", async (data) => {
      received.push(data.id);
    });

    await new Promise((r) => setTimeout(r, 200));
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
});
