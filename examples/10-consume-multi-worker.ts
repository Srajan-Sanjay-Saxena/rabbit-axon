/**
 * Example: Multi-Worker Consumer (Parallel Processing)
 *
 * Demonstrates:
 * - workerCount: 4 parallel consumers on separate channels
 * - prefetchCount: messages per worker at a time
 * - Each worker processes independently — no contention
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
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitProducerExchanger("notifications.exchange", {});

  await consumer.consumeMessage(
    rabbit,
    "emails.send",
    async (data, msg) => {
      const workerId = msg.fields.consumerTag;
      console.log(`[${workerId}] Sending email to ${data.to}`);

      await sendEmail(data.to, data.subject, data.body);

      console.log(`[${workerId}] Email sent ✓`);
    },
    {
      workerCount: 4,      // 4 parallel workers
      prefetchCount: 2,    // each worker grabs 2 messages at a time
    }
  );

  console.log("4 workers consuming from emails.send");

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

async function sendEmail(to: string, subject: string, body: string) {
  // simulate email sending latency
  await new Promise((r) => setTimeout(r, Math.random() * 2000));
}

main().catch(console.error);
