/**
 * Example: Dead Letter Exchange (DLX) — Proper Pattern
 *
 * Demonstrates:
 * - Separate DLX exchange (not polluting main exchange)
 * - Main queue routes failures to a dedicated DLX exchange
 * - DLQ consumer uses confirm channel to guarantee processing
 *
 * Architecture:
 *   payments.exchange (direct) ← main traffic
 *     └── "payment.process" → payments.process queue
 *                               ↓ (on nack/TTL expire)
 *   payments.dlx (direct) ← only dead letters
 *     └── "payment.failed" → payments.dlq queue
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
  RabbitConsumer,
} from "../src/index.js";

// --- Main Exchange ---

class PaymentExchange extends RabbitMqQueueExchange {
  static exchangeName = "payments.exchange";
  static exchangeType = "direct" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // Main processing queue — failures route to the DLX exchange
    await this.createDeadLetterQueue(
      "payments.process",
      PaymentDlxExchange.exchangeName, // ← separate DLX exchange
      "payment.failed",
      60000
    );

    console.log(`"${PaymentExchange.exchangeName}" setup complete`);
  }
}

// --- Separate DLX Exchange ---

class PaymentDlxExchange extends RabbitMqQueueExchange {
  static exchangeName = "payments.dlx";
  static exchangeType = "direct" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // DLQ where permanently failed messages land
    await this.createQueue("payments.dlq", "payment.failed", { durable: true });

    console.log(`"${PaymentDlxExchange.exchangeName}" DLX setup complete`);
  }
}

// --- DLQ Consumer with Confirm ---

async function startDlqConsumer(rabbit: RabbitMqBaseClass) {
  const dlqConsumer = new RabbitConsumer(PaymentDlxExchange.exchangeName);

  await dlqConsumer.consume(
    rabbit,
    "payments.dlq",
    async (data, msg) => {
      const retryCount = msg.properties.headers?.["x-retry-count"] ?? 0;
      console.log(`[DLQ] Dead letter received: ${data.paymentId} (failed ${retryCount} times)`);

      // Use confirm channel to safely move to a permanent store
      const confirmChannel = await rabbit.createConfirmChannel();
      try {
        // Example: republish to an audit exchange, or write to DB
        confirmChannel.publish(
          "",
          "payments.audit", // direct-to-queue publish for audit
          Buffer.from(JSON.stringify({
            ...data,
            failedAt: new Date().toISOString(),
            retryCount,
            reason: msg.properties.headers?.["x-first-death-reason"],
          })),
          { persistent: true }
        );

        await confirmChannel.waitForConfirms();
        console.log(`[DLQ] ${data.paymentId} safely stored in audit ✓`);
      } catch (err) {
        console.error(`[DLQ] Failed to store ${data.paymentId} — will retry`, err);
        throw err; // nack → stays in DLQ for retry
      } finally {
        await confirmChannel.close();
      }
    },
    { workerCount: 1, prefetchCount: 1, requeueOnFailure: true, retryLimit: 5 }
  );

  console.log("[DLQ] Consumer running with confirm channel");
}

// --- Main ---

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  // Setup DLX first (must exist before main queue references it)
  const dlxExchange = new PaymentDlxExchange();
  await dlxExchange.setup(rabbit);

  // Setup main exchange
  const mainExchange = new PaymentExchange();
  await mainExchange.setup(rabbit);

  // Start DLQ consumer with confirm guarantees
  await startDlqConsumer(rabbit);

  // Publish a test payment
  const producer = new RabbitProducer(PaymentExchange.exchangeName, "payment.process");
  await producer.publish(rabbit, {
    paymentId: "PAY-001",
    amount: 299.99,
    userId: "user-42",
  });
  console.log("\nTest payment published. If consumer nacks → DLX → DLQ → audit");

  console.log("\nArchitecture:");
  console.log(`  ${PaymentExchange.exchangeName} → payments.process`);
  console.log(`    ↓ (nack/TTL)`);
  console.log(`  ${PaymentDlxExchange.exchangeName} → payments.dlq → audit (confirmed)`);

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

main().catch(console.error);
