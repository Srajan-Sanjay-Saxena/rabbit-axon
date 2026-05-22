/**
 * Example: Produce Message (Fire-and-Forget)
 *
 * Demonstrates:
 * - Publishing a message to an exchange with routing key
 * - PublishOptions: persistent, priority, headers, messageId
 * - Channel is opened and closed automatically (try/finally)
 */

import {
  RabbitMqBaseClass,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");

  // Simple publish
  const simpleProducer = new RabbitProducerExchanger(
    "orders.exchange",
    { orderId: "ORD-001", item: "Widget", qty: 3 },
    "order.created"
  );

  await simpleProducer.produceMessage(rabbit);
  console.log("Simple message published");

  // Publish with full options
  const priorityProducer = new RabbitProducerExchanger(
    "orders.exchange",
    { orderId: "ORD-002", item: "Gadget", qty: 1, urgent: true },
    "order.created.priority"
  );

  await priorityProducer.produceMessage(rabbit, {
    persistent: true,          // survives broker restart
    priority: 9,               // high priority (if queue supports it)
    messageId: "msg-ORD-002",  // application-level dedup ID
    expiration: "300000",      // expires in 5 min if not consumed
    headers: {
      "x-source": "api-gateway",
      "x-tenant-id": "tenant-42",
    },
  });
  console.log("Priority message published with options");

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
