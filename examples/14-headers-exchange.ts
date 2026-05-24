/**
 * Example: Headers Exchange (Route by Headers, not Routing Key)
 *
 * Demonstrates:
 * - Exchange type "headers" — routing based on message headers
 * - Binding with x-match: "all" (all headers must match) or "any"
 * - Static exchangeName for producer reference
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
} from "../src/index.js";

class EventExchange extends RabbitMqQueueExchange {
  static exchangeName = "events.headers";
  static exchangeType = "headers" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // Only receives messages where BOTH headers match
    await this.createQueue(
      "events.critical-errors",
      "",
      { durable: true },
      undefined,
      {
        "x-match": "all",
        "severity": "critical",
        "type": "error",
      }
    );

    // Receives messages where ANY header matches
    await this.createQueue(
      "events.all-errors",
      "",
      { durable: true },
      undefined,
      {
        "x-match": "any",
        "type": "error",
        "severity": "critical",
      }
    );

    console.log(`"${EventExchange.exchangeName}" headers exchange setup complete`);
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const exchange = new EventExchange();
  await exchange.setup(rabbit);

  // Producer references static exchangeName — routing key ignored for headers
  const producer = new RabbitProducer(EventExchange.exchangeName, "");

  // Both queues receive this (all headers match)
  await producer.publish(
    rabbit,
    { message: "Database connection pool exhausted" },
    { headers: { severity: "critical", type: "error" } }
  );
  console.log("Critical error published → routes to both queues");

  // Only "events.all-errors" receives this (only type matches)
  await producer.publish(
    rabbit,
    { message: "High memory usage detected" },
    { headers: { severity: "warning", type: "error" } }
  );
  console.log("Warning published → routes to all-errors only");

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
