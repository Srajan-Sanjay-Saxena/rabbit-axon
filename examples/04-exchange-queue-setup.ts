/**
 * Example: Exchange & Queue Setup
 *
 * Demonstrates:
 * - Creating an exchange (topic type)
 * - Creating a queue with binding key
 * - Queue arguments: TTL, max-length, priority
 */

import { RabbitMqBaseClass, RabbitMqQueueExchange } from "../src";


class OrderExchange extends RabbitMqQueueExchange {
  constructor() {
    super("orders.exchange", "topic", { durable: true, autoDelete: false });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // Basic queue bound to a routing pattern
    await this.createQueue("orders.created", "order.created.#");

    // Queue with advanced arguments
    await this.createQueue(
      "orders.priority",
      "order.priority.*",
      { durable: true, autoDelete: false },
      {
        "x-message-ttl": 120000,    // messages expire after 2 min
        "x-max-length": 5000,       // max 5000 messages
        "x-max-priority": 10,       // enable priority (0-10)
        "x-queue-mode": "lazy",     // store to disk, save RAM
      }
    );

    console.log("Exchange and queues created successfully");
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const exchange = new OrderExchange();
  await exchange.setup(rabbit);

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
