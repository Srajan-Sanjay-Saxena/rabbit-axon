/**
 * Example: Queue Lifecycle — Delete, Purge, Unbind
 *
 * Demonstrates:
 * - deleteExchange() — remove an exchange
 * - deleteQueue() — remove a queue (with safety guards)
 * - purgeQueue() — clear all messages without deleting
 * - unbindQueue() — remove a binding between queue and exchange
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
} from "../Correct/Rabbit.singleton.correct";

class TempExchange extends RabbitMqQueueExchange {
  constructor() {
    super("temp.exchange", "topic", { durable: false, autoDelete: false });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();
    await this.createQueue("temp.queue", "temp.#");
    console.log("Setup done");
  }

  async demonstrateLifecycle() {
    // Purge: remove all messages but keep the queue
    await this.purgeQueue("temp.queue");
    console.log("Queue purged — all messages removed");

    // Unbind: detach queue from exchange (stops receiving new messages)
    await this.unbindQueue("temp.queue", "temp.#");
    console.log("Queue unbound from exchange");

    // Delete queue: only if empty AND no active consumers
    await this.deleteQueue("temp.queue", true, true);
    console.log("Queue deleted (was empty and unused)");

    // Delete exchange: only if no queues are bound
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
