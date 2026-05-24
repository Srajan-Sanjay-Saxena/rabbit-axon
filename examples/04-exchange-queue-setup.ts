/**
 * Example: Exchange & Queue Setup
 *
 * Demonstrates:
 * - Static exchangeName, exchangeType, exchangeOptions on the class
 * - Creating a queue with binding key
 * - Queue arguments: TTL, max-length, priority, lazy mode
 */

import { RabbitMqBaseClass, RabbitMqQueueExchange } from "../src/index.js";

class OrderExchange extends RabbitMqQueueExchange {
  static exchangeName = "orders.exchange";
  static exchangeType = "topic" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    await this.createQueue("orders.created", "order.created.#");

    await this.createQueue(
      "orders.priority",
      "order.priority.*",
      { durable: true, autoDelete: false },
      {
        "x-message-ttl": 120000,
        "x-max-length": 5000,
        "x-max-priority": 10,
        "x-queue-mode": "lazy",
      }
    );

    console.log(`Exchange "${OrderExchange.exchangeName}" and queues created`);
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
