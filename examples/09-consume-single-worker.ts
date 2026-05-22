/**
 * Example: Basic Consumer (Single Worker)
 *
 * Demonstrates:
 * - consumeMessage() with a single worker
 * - Handler receives parsed JSON + raw amqplib message
 * - Messages are ack'd on success, nack'd on failure → DLX
 */

import {
  RabbitMqBaseClass,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitProducerExchanger("orders.exchange", {});

  await consumer.consumeMessage(
    rabbit,
    "orders.created",
    async (data, msg) => {
      console.log("Received order:", data.orderId);
      console.log("  Routing key:", msg.fields.routingKey);
      console.log("  Timestamp:", msg.properties.timestamp);

      // Your business logic
      await processOrder(data);
    }
    // No options = defaults: workerCount=1, prefetch=1, no requeue
  );

  console.log("Consumer running. Waiting for messages...");

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

async function processOrder(data: Record<string, any>) {
  // simulate work
  await new Promise((r) => setTimeout(r, 500));
  console.log(`  Order ${data.orderId} processed ✓`);
}

main().catch(console.error);
