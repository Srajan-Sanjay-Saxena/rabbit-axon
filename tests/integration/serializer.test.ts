import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import {
  startRabbitContainer,
  stopRabbitContainer,
  getAmqpUrl,
} from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { RabbitMqQueueExchange } from "../../src/exchange/exchange.js";
import { RabbitProducer } from "../../src/producer/producer.js";
import { RabbitConsumer } from "../../src/consumer/consumer.js";
import {
  JsonSerializer,
  MsgpackSerializer,
  IdentitySerializer,
} from "../../src/serializer/serializer.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;
let handler: RabbitSingleConnectionHandler;

class SerializerExchange extends RabbitMqQueueExchange {
  constructor(handler: RabbitSingleConnectionHandler) {
    super(handler, "serializer.exchange", "direct");
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

describe("JsonSerializer", () => {
  it("serializes and deserializes correctly", () => {
    const s = new JsonSerializer();
    const data = { orderId: "ORD-1", amount: 99.99, nested: { region: "us" } };
    const buf = s.serialize(data);
    expect(Buffer.isBuffer(buf)).toBe(true);
    expect(s.deserialize(buf)).toEqual(data);
  });

  it("contentType is application/json", () => {
    expect(new JsonSerializer().contentType).toBe("application/json");
  });

  it("producer sets application/json contentType and consumer receives correct data", async () => {
    const exchange = new SerializerExchange(handler);
    await exchange.setup("s.json", "s.json.key");

    const producer = new RabbitProducer(
      handler,
      "serializer.exchange",
      "s.json.key",
      {
        serializer: new JsonSerializer(),
      },
    );
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<{
      contentType: string;
      data: Record<string, any>;
    }>((resolve) => {
      consumer.consume("s.json", async (data, msg) => {
        resolve({ contentType: msg.properties.contentType, data });
      });
    });

    await producer.publish({ orderId: "ORD-JSON" });

    const result = await received;
    expect(result.contentType).toBe("application/json");
    expect(result.data.orderId).toBe("ORD-JSON");
  });
});

describe("MsgpackSerializer", () => {
  it("serializes and deserializes correctly", () => {
    const s = new MsgpackSerializer();
    const data = { orderId: "ORD-1", amount: 99.99, nested: { region: "us" } };
    const buf = s.serialize(data);
    expect(Buffer.isBuffer(buf)).toBe(true);
    // msgpack is smaller than JSON for same data
    expect(buf.length).toBeLessThan(Buffer.from(JSON.stringify(data)).length);
    expect(s.deserialize(buf)).toEqual(data);
  });

  it("contentType is application/msgpack", () => {
    expect(new MsgpackSerializer().contentType).toBe("application/msgpack");
  });

  it("producer sets application/msgpack contentType and consumer auto-detects and deserializes", async () => {
    const exchange = new SerializerExchange(handler);
    await exchange.setup("s.msgpack", "s.msgpack.key");

    const producer = new RabbitProducer(
      handler,
      "serializer.exchange",
      "s.msgpack.key",
      {
        serializer: new MsgpackSerializer(),
      },
    );
    // consumer uses default JSON serializer — but auto-detects msgpack from contentType
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<{
      contentType: string;
      data: Record<string, any>;
    }>((resolve) => {
      consumer.consume("s.msgpack", async (data, msg) => {
        resolve({ contentType: msg.properties.contentType, data });
      });
    });

    await producer.publish({ orderId: "ORD-MSGPACK", amount: 42 });

    const result = await received;
    expect(result.contentType).toBe("application/msgpack");
    expect(result.data.orderId).toBe("ORD-MSGPACK");
    expect(result.data.amount).toBe(42);
  });
});

describe("IdentitySerializer", () => {
  it("passes Buffer through unchanged", () => {
    const s = new IdentitySerializer();
    const buf = Buffer.from([0x01, 0x02, 0x03]);
    expect(s.serialize(buf)).toBe(buf);
    expect(s.deserialize(buf)).toBe(buf);
  });

  it("throws if data is not a Buffer", () => {
    const s = new IdentitySerializer();
    expect(() => s.serialize({ orderId: "ORD-1" })).toThrow(
      "[IdentitySerializer]",
    );
  });

  it("contentType is application/octet-stream", () => {
    expect(new IdentitySerializer().contentType).toBe(
      "application/octet-stream",
    );
  });

  it("producer sends raw Buffer and consumer receives raw Buffer", async () => {
    const exchange = new SerializerExchange(handler);
    await exchange.setup("s.identity", "s.identity.key");

    const producer = new RabbitProducer<{ data: Buffer }>(
      handler,
      "serializer.exchange",
      "s.identity.key",
      {
        serializer: new IdentitySerializer(),
      },
    );
    const consumer = new RabbitConsumer(handler, {
      serializer: new IdentitySerializer(),
    });

    const originalBytes = Buffer.from([0xde, 0xad, 0xbe, 0xef]);

    const received = new Promise<Buffer>((resolve) => {
      consumer.consume("s.identity", async (data) => {
        resolve(data as unknown as Buffer);
      });
    });

    await producer.publish(originalBytes as any);

    const result = await received;
    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result).toEqual(originalBytes);
  });
});

describe("Serializer auto-detection", () => {
  it("consumer with JSON default correctly deserializes msgpack message via contentType", async () => {
    const exchange = new SerializerExchange(handler);
    await exchange.setup("s.autodetect", "s.autodetect.key");

    // producer uses msgpack
    const producer = new RabbitProducer(
      handler,
      "serializer.exchange",
      "s.autodetect.key",
      {
        serializer: new MsgpackSerializer(),
      },
    );
    // consumer uses JSON default — should still work via auto-detection
    const consumer = new RabbitConsumer(handler);

    const received = new Promise<Record<string, any>>((resolve) => {
      consumer.consume("s.autodetect", async (data) => resolve(data));
    });

    await producer.publish({ id: "auto-1", value: 123 });

    const result = await received;
    expect(result.id).toBe("auto-1");
    expect(result.value).toBe(123);
  });

  it("mixed producers — consumer handles both JSON and msgpack messages correctly", async () => {
    const exchange = new SerializerExchange(handler);
    await exchange.setup("s.mixed", "s.mixed.key");

    const jsonProducer = new RabbitProducer(
      handler,
      "serializer.exchange",
      "s.mixed.key",
    );
    const msgpackProducer = new RabbitProducer(
      handler,
      "serializer.exchange",
      "s.mixed.key",
      {
        serializer: new MsgpackSerializer(),
      },
    );
    const consumer = new RabbitConsumer(handler);
    const received: string[] = [];

    await consumer.consume("s.mixed", async (data) => {
      received.push(data.id);
    });

    await jsonProducer.publish({ id: "json-msg" });
    await msgpackProducer.publish({ id: "msgpack-msg" });

    await new Promise((r) => setTimeout(r, 500));

    expect(received).toContain("json-msg");
    expect(received).toContain("msgpack-msg");
  });
});
