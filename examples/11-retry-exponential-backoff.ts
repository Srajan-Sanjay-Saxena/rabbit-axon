/**
 * Example: Retry with Exponential Backoff
 *
 * Demonstrates:
 * - requeueOnFailure: true — failed messages are retried
 * - retryLimit: 3 — after 3 retries, message goes to DLX
 * - Backoff: 1s → 2s → 4s (capped at 30s)
 * - Retry count tracked via x-retry-count header
 *
 * Flow:
 *   handler throws → republish with x-retry-count+1 and TTL delay
 *   after retryLimit → nack without requeue → DLX picks it up
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitConsumer,
} from "../src/index.js";

class PaymentExchange extends RabbitMqQueueExchange {
  static exchangeName = "payments.exchange";
  static exchangeType = "direct" as const;
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitConsumer(PaymentExchange.exchangeName);

  await consumer.consume(
    rabbit,
    "payments.process",
    async (data, msg) => {
      const attempt = (msg.properties.headers?.["x-retry-count"] ?? 0) + 1;
      console.log(`Processing payment ${data.paymentId} (attempt ${attempt})`);

      if (Math.random() < 0.7) {
        throw new Error("Payment gateway timeout");
      }

      console.log(`Payment ${data.paymentId} processed ✓`);
    },
    {
      workerCount: 2,
      prefetchCount: 1,
      requeueOnFailure: true,
      retryLimit: 3,
    }
  );

  console.log("Consumer with retry running...");
  console.log("Retry schedule: 1s → 2s → 4s → DLX");

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

main().catch(console.error);
