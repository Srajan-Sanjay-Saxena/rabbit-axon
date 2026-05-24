/**
 * Example: Auto-Reconnection with Full Re-initialization
 *
 * Demonstrates:
 * - onReconnect() to re-establish exchanges, queues, and consumers after connection drops
 * - Static exchangeName accessed directly from the class
 * - Complete lifecycle: setup → consume → reconnect → re-setup → resume consuming
 *
 * Test this by:
 *   1. Start this script
 *   2. Restart RabbitMQ: `docker restart rabbitmq`
 *   3. Watch it reconnect and resume consuming automatically
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
  RabbitConsumer,
} from "../src/index.js";

class OrderExchange extends RabbitMqQueueExchange {
  static exchangeName = "orders.exchange";
  static exchangeType = "topic" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();
    await this.createQueue("orders.dlq", "order.failed.#", { durable: true });
    await this.createDeadLetterQueue(
      "orders.process",
      OrderExchange.exchangeName,
      "order.failed",
      60000
    );
    console.log("[Setup] Exchange and queues asserted");
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost", {
    heartbeat: 10,
    reconnectInterval: 2000,
    maxReconnectAttempts: 20,
  });

  await rabbit.ConnectToService();
  console.log("[Main] Connected to RabbitMQ");

  const exchange = new OrderExchange();
  await exchange.setup(rabbit);

  // Use static exchangeName directly — no need to pass strings around
  const consumer = new RabbitConsumer(OrderExchange.exchangeName);
  await consumer.consume(
    rabbit,
    "orders.process",
    async (data, msg) => {
      const attempt = (msg.properties.headers?.["x-retry-count"] ?? 0) + 1;
      console.log(`[Consumer] Processing order ${data.orderId} (attempt ${attempt})`);
      await new Promise((r) => setTimeout(r, 300));
      console.log(`[Consumer] Order ${data.orderId} done ✓`);
    },
    { workerCount: 2, prefetchCount: 1, requeueOnFailure: true, retryLimit: 3 }
  );

  rabbit.onReconnect(async () => {
    console.log("[Reconnect] Connection restored. Re-asserting exchanges...");
    await exchange.setup(rabbit);
    console.log("[Reconnect] All resources re-initialized ✓");
  });

  // Producer also uses static exchangeName
  const producer = new RabbitProducer(OrderExchange.exchangeName, "order.created");
  setInterval(async () => {
    try {
      await producer.publish(rabbit, { orderId: `ORD-${Date.now()}`, item: "Widget" });
      console.log("[Publisher] Test message sent");
    } catch (err) {
      console.warn("[Publisher] Failed:", (err as Error).message);
    }
  }, 5000);

  const shutdown = async () => {
    console.log("\n[Shutdown] Closing connection...");
    await rabbit.gracefulShutdown();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  console.log("\n[Main] Service running. Press Ctrl+C to stop.");
  console.log("[Main] Restart RabbitMQ to test reconnection.\n");
}

main().catch(console.error);
