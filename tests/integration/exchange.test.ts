import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { RabbitMqQueueExchange } from "../../src/exchange/exchange.js";
import { RabbitProducer } from "../../src/producer/producer.js";
import { RabbitConsumer } from "../../src/consumer/consumer.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;
let handler: RabbitSingleConnectionHandler;

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

class DirectExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "ex.direct", "direct");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("ex.direct.errors", "error");
    await this.createQueue("ex.direct.warnings", "warning");
    await this.createQueue("ex.direct.info", "info");
  }
}

class FanoutExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "ex.fanout", "fanout");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("ex.fanout.a", "");
    await this.createQueue("ex.fanout.b", "");
    await this.createQueue("ex.fanout.c", "");
  }
}

class TopicExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "ex.topic", "topic");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("ex.topic.all", "order.#");
    await this.createQueue("ex.topic.created", "order.created.*");
    await this.createQueue("ex.topic.us", "order.*.us");
    await this.createQueue("ex.topic.eu", "order.*.eu");
  }
}

class DlxExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "ex.dlx", "direct");
  }
  async setup() {
    await this.createExchange();
    await this.createQueue("ex.dlx.dead", "dead");
    await this.createQueue("ex.dlx.main", "main", { durable: true }, {
      "x-dead-letter-exchange": "ex.dlx",
      "x-dead-letter-routing-key": "dead",
      "x-message-ttl": 500,
    });
  }
}

describe("DirectExchange", () => {
  it("routes to exact matching queue only — other queues receive nothing", async () => {
    const exchange = new DirectExchange(handler);
    await exchange.setup();

    const producer = new RabbitProducer(handler, "ex.direct", "error");
    const consumer = new RabbitConsumer(handler);

    const errorReceived = new Promise<void>((resolve) => {
      consumer.consume("ex.direct.errors", async () => resolve());
    });

    const warningHandler = vi.fn();
    const infoHandler = vi.fn();
    await consumer.consume("ex.direct.warnings", async () => { warningHandler(); });
    await consumer.consume("ex.direct.info", async () => { infoHandler(); });

    await producer.publish({ msg: "disk full", severity: "error" });

    await errorReceived;

    await new Promise((r) => setTimeout(r, 500));
    expect(warningHandler).not.toHaveBeenCalled();
    expect(infoHandler).not.toHaveBeenCalled();
  });

  it("each routing key delivers to its own queue independently", async () => {
    const exchange = new DirectExchange(handler);
    await exchange.setup();

    const errorProducer = new RabbitProducer(handler, "ex.direct", "error");
    const warnProducer = new RabbitProducer(handler, "ex.direct", "warning");
    const consumer = new RabbitConsumer(handler);

    const errors: string[] = [];
    const warnings: string[] = [];

    await consumer.consume("ex.direct.errors", async (data) => { errors.push(data.id); });
    await consumer.consume("ex.direct.warnings", async (data) => { warnings.push(data.id); });

    await errorProducer.publish({ id: "e1" });
    await errorProducer.publish({ id: "e2" });
    await warnProducer.publish({ id: "w1" });

    await new Promise((r) => setTimeout(r, 500));

    expect(errors).toEqual(expect.arrayContaining(["e1", "e2"]));
    expect(warnings).toEqual(["w1"]);
    expect(errors).not.toContain("w1");
    expect(warnings).not.toContain("e1");
  });
});

describe("FanoutExchange", () => {
  it("broadcasts to all queues — every subscriber gets every message", async () => {
    const exchange = new FanoutExchange(handler);
    await exchange.setup();

    const producer = new RabbitProducer(handler, "ex.fanout", "");
    const consumer = new RabbitConsumer(handler);

    const receivedA: string[] = [];
    const receivedB: string[] = [];
    const receivedC: string[] = [];

    await consumer.consume("ex.fanout.a", async (data) => { receivedA.push(data.id); });
    await consumer.consume("ex.fanout.b", async (data) => { receivedB.push(data.id); });
    await consumer.consume("ex.fanout.c", async (data) => { receivedC.push(data.id); });

    await producer.publish({ id: "broadcast-1" });
    await producer.publish({ id: "broadcast-2" });

    await new Promise((r) => setTimeout(r, 500));

    expect(receivedA).toEqual(expect.arrayContaining(["broadcast-1", "broadcast-2"]));
    expect(receivedB).toEqual(expect.arrayContaining(["broadcast-1", "broadcast-2"]));
    expect(receivedC).toEqual(expect.arrayContaining(["broadcast-1", "broadcast-2"]));
  });
});

describe("TopicExchange", () => {
  it("routes to all pattern-matching queues and none of the non-matching ones", async () => {
    const exchange = new TopicExchange(handler);
    await exchange.setup();

    const producer = new RabbitProducer(handler, "ex.topic", "order.created.us");
    const consumer = new RabbitConsumer(handler);

    const allReceived = new Promise<string>((resolve) => {
      consumer.consume("ex.topic.all", async (data) => resolve(data.orderId));
    });
    const createdReceived = new Promise<string>((resolve) => {
      consumer.consume("ex.topic.created", async (data) => resolve(data.orderId));
    });
    const usReceived = new Promise<string>((resolve) => {
      consumer.consume("ex.topic.us", async (data) => resolve(data.orderId));
    });
    const euHandler = vi.fn();
    await consumer.consume("ex.topic.eu", async () => { euHandler(); });

    await producer.publish({ orderId: "ORD-US-1" });

    const [all, created, us] = await Promise.all([allReceived, createdReceived, usReceived]);
    expect(all).toBe("ORD-US-1");
    expect(created).toBe("ORD-US-1");
    expect(us).toBe("ORD-US-1");

    await new Promise((r) => setTimeout(r, 300));
    expect(euHandler).not.toHaveBeenCalled();
  });

  it("order.shipped.eu matches order.# and order.*.eu but not order.created.*", async () => {
    const exchange = new TopicExchange(handler);
    await exchange.setup();

    const producer = new RabbitProducer(handler, "ex.topic", "order.shipped.eu");
    const consumer = new RabbitConsumer(handler);

    const allReceived = new Promise<string>((resolve) => {
      consumer.consume("ex.topic.all", async (data) => resolve(data.orderId));
    });
    const euReceived = new Promise<string>((resolve) => {
      consumer.consume("ex.topic.eu", async (data) => resolve(data.orderId));
    });
    const createdHandler = vi.fn();
    const usHandler = vi.fn();
    await consumer.consume("ex.topic.created", async () => { createdHandler(); });
    await consumer.consume("ex.topic.us", async () => { usHandler(); });

    await producer.publish({ orderId: "ORD-EU-1" });

    const [all, eu] = await Promise.all([allReceived, euReceived]);
    expect(all).toBe("ORD-EU-1");
    expect(eu).toBe("ORD-EU-1");

    await new Promise((r) => setTimeout(r, 300));
    expect(createdHandler).not.toHaveBeenCalled();
    expect(usHandler).not.toHaveBeenCalled();
  });
});

describe("DeadLetterExchange", () => {
  it("message expires via TTL and is routed to DLX queue with original payload", async () => {
    const exchange = new DlxExchange(handler);
    await exchange.setup();

    const producer = new RabbitProducer(handler, "ex.dlx", "main");
    const consumer = new RabbitConsumer(handler);

    const deadReceived = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("ex.dlx.dead", async (data) => resolve(data));
    });

    await producer.publish({ orderId: "ORD-EXPIRED", amount: 50 });

    const dead = await deadReceived;
    expect(dead.orderId).toBe("ORD-EXPIRED");
    expect(dead.amount).toBe(50);
  });

  it("throws when no active connection", async () => {
    const handler2 = new RabbitSingleConnectionHandler(amqpUrl, {
      circuitBreaker: { threshold: 0, resetTimeout: 60000 },
    });
    await handler2.ConnectToService();
    await handler2.gracefulShutdown();

    class BadExchange extends RabbitMqQueueExchange {
      constructor() { super(handler2, "bad", "direct"); }
      async setup() { await this.createExchange(); }
    }

    await expect(new BadExchange().setup()).rejects.toThrow("[Exchange] No active connection");
  });
});
