/**
 * Example: Queue Lifecycle — Delete, Purge, Unbind
 *
 * Demonstrates:
 * - deleteExchange() — remove an exchange
 * - deleteQueue() — remove a queue (with safety guards)
 * - purgeQueue() — clear all messages without deleting
 * - unbindQueue() — remove a binding between queue and exchange
 */

import { RabbitMqBaseClass, RabbitMqQueueExchange } from "../src/index.js";

class TempExchange extends RabbitMqQueueExchange {
  static exchangeName = "temp.exchange";
  static exchangeType = "topic" as const;
  static exchangeOptions = { durable: false, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();
    await this.createQueue("temp.queue", "temp.#");
    console.log(`"${TempExchange.exchangeName}" setup done`);
  }

  async demonstrateLifecycle() {
    await this.purgeQueue("temp.queue");
    console.log("Queue purged — all messages removed");

    await this.unbindQueue("temp.queue", "temp.#");
    console.log("Queue unbound from exchange");

    await this.deleteQueue("temp.queue", true, true);
    console.log("Queue deleted (was empty and unused)");

    await this.deleteExchange(true);
    console.log("Exchange deleted (was unused)");
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const exchange = new TempExchange();
  await exchange.setup(rabbit);
  await exchange.demonstrateLifecycle();

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
