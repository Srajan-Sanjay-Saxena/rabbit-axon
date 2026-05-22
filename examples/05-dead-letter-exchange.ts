/**
 * Example: Dead Letter Exchange (DLX)
 *
 * Demonstrates:
 * - Creating a DLX exchange + parking queue for failed messages
 * - Creating a main queue that routes failures to DLX
 * - Messages nack'd or expired → automatically land in DLQ
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
} from "../Correct/Rabbit.singleton.correct";

class PaymentExchange extends RabbitMqQueueExchange {
  constructor() {
    super("payments.exchange", "direct", { durable: true });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // 1. Create the DLQ (dead-letter queue) where failed messages land
    await this.createQueue("payments.dlq", "payment.failed", {
      durable: true,
    });

    // 2. Create the main processing queue with DLX configured
    //    Failed/expired messages → routed to "payments.exchange" with key "payment.failed"
    await this.createDeadLetterQueue(
      "payments.process",      // queue name
      "payments.exchange",     // DLX exchange
      "payment.failed",        // DLX routing key
      60000                    // TTL: 60 seconds
    );

    console.log("DLX setup complete:");
    console.log("  payments.process → (on failure) → payments.dlq");
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const exchange = new PaymentExchange();
  await exchange.setup(rabbit);

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
