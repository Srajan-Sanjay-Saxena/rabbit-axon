/**
 * Example: Produce Message (Fire-and-Forget)
 *
 * Demonstrates:
 * - Using static exchangeName from the exchange class
 * - PublishOptions: persistent, priority, headers, messageId
 * - Producer is reusable — data passed at publish time
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
} from "../src/index.js";

class OrderExchange extends RabbitMqQueueExchange {
  static exchangeName = "orders.exchange";
  static exchangeType = "topic" as const;
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");

  // Reference static exchangeName — no magic strings
  const orderProducer = new RabbitProducer(OrderExchange.exchangeName, "order.created");

  await orderProducer.publish(rabbit, {
    orderId: "ORD-001",
    item: "Widget",
    qty: 3,
  });
  console.log("Simple message published");

  await orderProducer.publish(
    rabbit,
    { orderId: "ORD-002", item: "Gadget", qty: 1, urgent: true },
    {
      persistent: true,
      priority: 9,
      messageId: "msg-ORD-002",
      expiration: "300000",
      headers: {
        "x-source": "api-gateway",
        "x-tenant-id": "tenant-42",
      },
    }
  );
  console.log("Priority message published with options");

  await orderProducer.publish(rabbit, {
    orderId: "ORD-003",
    item: "Doohickey",
    qty: 10,
  });
  console.log("Third message published (same producer reused)");

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
