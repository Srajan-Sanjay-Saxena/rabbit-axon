import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { RabbitConnectionPoolHandler } from "../../src/connection/pool.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;

beforeAll(async () => {
  container = await startRabbitContainer();
  amqpUrl = getAmqpUrl(container);
});

afterAll(async () => {
  await stopRabbitContainer(container);
});

describe("RabbitSingleConnectionHandler", () => {
  it("connects, exposes live connection, shuts down cleanly and nulls connection", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl);
    await handler.ConnectToService();

    expect(handler.rabbitConnection).not.toBeNull();

    await handler.gracefulShutdown();

    // connection must be null after shutdown — no stale reference
    expect(handler.rabbitConnection).toBeNull();
  });

  it("can reconnect after graceful shutdown — state is fully reset", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl);
    await handler.ConnectToService();
    await handler.gracefulShutdown();

    // should be able to connect again cleanly
    await handler.ConnectToService();
    expect(handler.rabbitConnection).not.toBeNull();

    await handler.gracefulShutdown();
  });

  it("nulls connection on drop and fires all onReconnect callbacks exactly once", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      circuitBreaker: { threshold: 5, resetTimeout: 300 },
    });
    await handler.ConnectToService();

    const cb1 = vi.fn().mockResolvedValue(undefined);
    const cb2 = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb1);
    handler.onReconnect(cb2);

    const connBefore = handler.rabbitConnection;

    // force drop
    await handler.rabbitConnection!.close();

    // immediately after drop, connection should be null
    expect(handler.rabbitConnection).toBeNull();

    // wait for reconnect
    await new Promise((r) => setTimeout(r, 1000));

    // new connection should be a different object
    expect(handler.rabbitConnection).not.toBeNull();
    expect(handler.rabbitConnection).not.toBe(connBefore);

    // each callback fired exactly once
    expect(cb1).toHaveBeenCalledTimes(1);
    expect(cb2).toHaveBeenCalledTimes(1);

    await handler.gracefulShutdown();
  });

  it("does not reconnect after graceful shutdown when connection closes", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      circuitBreaker: { threshold: 5, resetTimeout: 300 },
    });
    await handler.ConnectToService();

    const cb = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb);

    await handler.gracefulShutdown();

    // wait to confirm no reconnect happens
    await new Promise((r) => setTimeout(r, 600));

    expect(cb).not.toHaveBeenCalled();
    expect(handler.rabbitConnection).toBeNull();
  });

  it("circuit opens after threshold failures and stops attempting reconnect", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      // threshold=0 — circuit opens immediately on first failure before any attempt
      // resetTimeout=60s — probe won't fire during this test
      circuitBreaker: { threshold: 0, resetTimeout: 60000 },
    });
    await handler.ConnectToService();

    const cb = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb);

    await handler.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 500));

    // circuit is OPEN — no reconnect happened
    expect(handler.rabbitConnection).toBeNull();
    expect(cb).not.toHaveBeenCalled();

    await handler.gracefulShutdown();
  });
});

describe("RabbitConnectionPoolHandler", () => {
  it("initializes pool and rabbitConnection is active", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    expect(pool.rabbitConnection).not.toBeNull();

    await pool.gracefulShutdown();
  });

  it("getChannel round-robins — returns different channels per connection", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    const ch1 = await pool.getChannel();
    const ch2 = await pool.getChannel();
    const ch3 = await pool.getChannel();

    expect(ch1).toBeDefined();
    expect(ch2).toBeDefined();
    expect(ch3).toBeDefined();

    // 4th call wraps around — reuses ch1
    const ch4 = await pool.getChannel();
    expect(ch4).toBe(ch1);

    await pool.gracefulShutdown();
  });

  it("rabbitConnection returns null when all connections are dropped", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 2, {
      circuitBreaker: { threshold: 0, resetTimeout: 60000 },
    });
    await pool.ConnectToService();

    const conn1 = pool.rabbitConnection!;
    await conn1.close();
    await new Promise((r) => setTimeout(r, 200));

    const conn2 = pool.rabbitConnection!;
    await conn2.close();
    await new Promise((r) => setTimeout(r, 200));

    expect(pool.rabbitConnection).toBeNull();

    await pool.gracefulShutdown();
  });

  it("shuts down all connections and clears pool", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();
    await pool.gracefulShutdown();

    expect(pool.rabbitConnection).toBeNull();
  });
});
