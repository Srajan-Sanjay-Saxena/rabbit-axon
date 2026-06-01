import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { CircuitBreaker } from "../../src/connection/circuit-breaker.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;

beforeAll(async () => {
  container = await startRabbitContainer();
  amqpUrl = getAmqpUrl(container);
});

afterAll(async () => {
  await stopRabbitContainer(container);
});

// ─── CircuitBreaker unit tests ────────────────────────────────────────────────

describe("CircuitBreaker — state transitions", () => {
  it("starts in CLOSED state", () => {
    const breaker = new CircuitBreaker();
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.canAttempt()).toBe(true);
  });

  it("stays CLOSED below threshold", () => {
    const breaker = new CircuitBreaker({ threshold: 3 });
    breaker.recordFailure();
    breaker.recordFailure();
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.canAttempt()).toBe(true);
  });

  it("opens after consecutive failures hit threshold", () => {
    const breaker = new CircuitBreaker({ threshold: 3 });
    breaker.recordFailure();
    breaker.recordFailure();
    breaker.recordFailure();
    expect(breaker.getState()).toBe("OPEN");
    expect(breaker.canAttempt()).toBe(false);
  });

  it("recordSuccess resets to CLOSED and clears failure count", () => {
    const breaker = new CircuitBreaker({ threshold: 2 });
    breaker.recordFailure();
    breaker.recordFailure(); // OPEN
    expect(breaker.getState()).toBe("OPEN");

    breaker.recordSuccess();
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.canAttempt()).toBe(true);

    // failure count reset — needs threshold failures again to open
    breaker.recordFailure();
    expect(breaker.getState()).toBe("CLOSED");
  });

  it("HALF_OPEN failure immediately goes back to OPEN regardless of count", () => {
    const breaker = new CircuitBreaker({ threshold: 5, resetTimeout: 50 });

    // open the circuit
    for (let i = 0; i < 5; i++) breaker.recordFailure();
    expect(breaker.getState()).toBe("OPEN");

    // manually transition to HALF_OPEN via scheduleProbe
    return new Promise<void>((resolve) => {
      breaker.scheduleProbe(() => {
        expect(breaker.getState()).toBe("HALF_OPEN");
        // one failure in HALF_OPEN → immediately OPEN
        breaker.recordFailure();
        expect(breaker.getState()).toBe("OPEN");
        expect(breaker.canAttempt()).toBe(false);
        breaker.clearProbeTimer();
        resolve();
      });
    });
  });

  it("HALF_OPEN success transitions to CLOSED", () => {
    const breaker = new CircuitBreaker({ threshold: 3, resetTimeout: 50 });
    for (let i = 0; i < 3; i++) breaker.recordFailure();
    expect(breaker.getState()).toBe("OPEN");

    return new Promise<void>((resolve) => {
      breaker.scheduleProbe(() => {
        expect(breaker.getState()).toBe("HALF_OPEN");
        breaker.recordSuccess();
        expect(breaker.getState()).toBe("CLOSED");
        expect(breaker.canAttempt()).toBe(true);
        resolve();
      });
    });
  });

  it("reset() returns to CLOSED and cancels probe timer", () => {
    const breaker = new CircuitBreaker({ threshold: 1, resetTimeout: 10000 });
    breaker.recordFailure(); // OPEN
    expect(breaker.getState()).toBe("OPEN");

    const probe = vi.fn();
    breaker.scheduleProbe(probe);

    breaker.reset();
    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.canAttempt()).toBe(true);

    // probe should never fire after reset
    return new Promise<void>((resolve) => {
      setTimeout(() => {
        expect(probe).not.toHaveBeenCalled();
        resolve();
      }, 200);
    });
  });
});

describe("CircuitBreaker — exponential backoff with jitter", () => {
  it("getBackoffDelay returns value within [0, min(1000*2^attempt, maxInterval)]", () => {
    const breaker = new CircuitBreaker();
    const maxInterval = 5000;

    for (let attempt = 0; attempt < 6; attempt++) {
      const delay = breaker.getBackoffDelay(attempt, maxInterval);
      const cap = Math.min(1000 * 2 ** attempt, maxInterval);
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThanOrEqual(cap);
    }
  });

  it("delay is capped at maxInterval for large attempt numbers", () => {
    const breaker = new CircuitBreaker();
    const maxInterval = 5000;
    // attempt 10 → 1000 * 2^10 = 1024000 >> 5000
    const delay = breaker.getBackoffDelay(10, maxInterval);
    expect(delay).toBeLessThanOrEqual(maxInterval);
  });
});

describe("CircuitBreaker — probe scheduling and resetTimeout doubling", () => {
  it("scheduleProbe fires after resetTimeout and transitions to HALF_OPEN", () => {
    const breaker = new CircuitBreaker({ threshold: 1, resetTimeout: 100 });
    breaker.recordFailure(); // OPEN

    return new Promise<void>((resolve) => {
      const start = Date.now();
      breaker.scheduleProbe(() => {
        const elapsed = Date.now() - start;
        expect(breaker.getState()).toBe("HALF_OPEN");
        expect(elapsed).toBeGreaterThanOrEqual(90); // allow small timer variance
        breaker.clearProbeTimer();
        resolve();
      });
    });
  });

  it("resetTimeout doubles on each OPEN cycle", () => {
    const breaker = new CircuitBreaker({ threshold: 1, resetTimeout: 100, maxResetTimeout: 1000 });
    breaker.recordFailure(); // OPEN

    return new Promise<void>((resolve) => {
      // 1st probe — fires after 100ms, currentResetTimeout becomes 200ms
      breaker.scheduleProbe(() => {
        breaker.recordFailure(); // HALF_OPEN fail → OPEN again

        const start = Date.now();
        // 2nd probe — should fire after 200ms
        breaker.scheduleProbe(() => {
          const elapsed = Date.now() - start;
          expect(elapsed).toBeGreaterThanOrEqual(180);
          breaker.clearProbeTimer();
          resolve();
        });
      });
    });
  }, 10000);

  it("resetTimeout is capped at maxResetTimeout", () => {
    const breaker = new CircuitBreaker({ threshold: 1, resetTimeout: 100, maxResetTimeout: 150 });
    breaker.recordFailure();

    return new Promise<void>((resolve) => {
      breaker.scheduleProbe(() => {
        // after 1st probe, currentResetTimeout would be 200 but capped at 150
        breaker.recordFailure(); // back to OPEN

        const start = Date.now();
        breaker.scheduleProbe(() => {
          const elapsed = Date.now() - start;
          // should fire around 150ms, not 200ms
          expect(elapsed).toBeGreaterThanOrEqual(130);
          expect(elapsed).toBeLessThan(250);
          breaker.clearProbeTimer();
          resolve();
        });
      });
    });
  }, 10000);
});

// ─── Integration tests with RabbitSingleConnectionHandler ────────────────────

describe("CircuitBreaker — integration with RabbitSingleConnectionHandler", () => {
  it("circuit opens after threshold and connection stays null — no reconnect spam", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
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

  it("reconnects successfully after probe fires — onReconnect callbacks fire", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      // threshold=1 so circuit opens immediately on first failure
      // resetTimeout=300ms so probe fires quickly in test
      circuitBreaker: { threshold: 1, resetTimeout: 300 },
    });
    await handler.ConnectToService();

    const cb = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb);

    await handler.rabbitConnection!.close();

    // wait for circuit to open, probe to fire, and reconnect to succeed
    await new Promise((r) => setTimeout(r, 1500));

    expect(handler.rabbitConnection).not.toBeNull();
    expect(cb).toHaveBeenCalledTimes(1);

    await handler.gracefulShutdown();
  });

  it("gracefulShutdown cancels pending probe — no reconnect after shutdown", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      // threshold=0 opens immediately, resetTimeout=500ms so probe would fire during test
      circuitBreaker: { threshold: 0, resetTimeout: 500 },
    });
    await handler.ConnectToService();

    const cb = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb);

    await handler.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 100)); // circuit opens, probe scheduled

    await handler.gracefulShutdown(); // should cancel the probe timer

    // wait past the resetTimeout to confirm probe never fires
    await new Promise((r) => setTimeout(r, 500));

    expect(cb).not.toHaveBeenCalled();
    expect(handler.rabbitConnection).toBeNull();
  });
});
