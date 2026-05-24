/**
 * Example: Multi-Worker Consumer (Parallel Processing)
 *
 * Demonstrates:
 * - workerCount: 4 parallel consumers on separate channels
 * - prefetchCount: messages per worker at a time
 * - Static exchangeName — no string duplication
 *
 * Architecture:
 *   Queue: emails.send
 *     ├── Worker 0 (Channel 1, prefetch=2) → handler()
 *     ├── Worker 1 (Channel 2, prefetch=2) → handler()
 *     ├── Worker 2 (Channel 3, prefetch=2) → handler()
 *     └── Worker 3 (Channel 4, prefetch=2) → handler()
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitConsumer,
} from "../src/index.js";

class NotificationExchange extends RabbitMqQueueExchange {
  static exchangeName = "notifications.exchange";
  static exchangeType = "direct" as const;
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitConsumer(NotificationExchange.exchangeName);

  await consumer.consume(
    rabbit,
    "emails.send",
    async (data, msg) => {
      const workerId = msg.fields.consumerTag;
      console.log(`[${workerId}] Sending email to ${data.to}`);

      await sendEmail(data.to, data.subject, data.body);

      console.log(`[${workerId}] Email sent ✓`);
    },
    {
      workerCount: 4,
      prefetchCount: 2,
    }
  );

  console.log(`4 workers consuming from "${NotificationExchange.exchangeName}"`);

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

async function sendEmail(to: string, subject: string, body: string) {
  await new Promise((r) => setTimeout(r, Math.random() * 2000));
}

main().catch(console.error);
