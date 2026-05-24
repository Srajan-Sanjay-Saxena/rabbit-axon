/**
 * Example: Basic Consumer (Single Worker)
 *
 * Demonstrates:
 * - RabbitConsumer with static exchangeName
 * - Handler receives parsed JSON + raw amqplib message
 * - Messages are ack'd on success, nack'd on failure → DLX
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitConsumer,
} from "../src/index.js";

class OrderExchange extends RabbitMqQueueExchange {
  static exchangeName = "orders.exchange";
  static exchangeType = "topic" as const;
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitConsumer(OrderExchange.exchangeName);

  await consumer.consume(
    rabbit,
    "orders.created",
    async (data, msg) => {
      console.log("Received order:", data.orderId);
      console.log("  Routing key:", msg.fields.routingKey);
      console.log("  Timestamp:", msg.properties.timestamp);

      await processOrder(data);
    }
  );

  console.log(`Consumer running on "${OrderExchange.exchangeName}". Waiting for messages...`);

  process.on("SIGINT", async () => {
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

async function processOrder(data: Record<string, any>) {
  await new Promise((r) => setTimeout(r, 500));
  console.log(`  Order ${data.orderId} processed ✓`);
}

main().catch(console.error);
