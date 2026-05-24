/**
 * Example: Produce with Confirm (Guaranteed Delivery)
 *
 * Demonstrates:
 * - publishWithConfirm() — broker acknowledges receipt
 * - Returns true if confirmed, false if nacked
 * - Static exchangeName — single source of truth
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
} from "../src/index.js";

class PaymentExchange extends RabbitMqQueueExchange {
  static exchangeName = "payments.exchange";
  static exchangeType = "direct" as const;
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");

  const paymentProducer = new RabbitProducer(PaymentExchange.exchangeName, "payment.process");

  const confirmed = await paymentProducer.publishWithConfirm(
    rabbit,
    {
      paymentId: "PAY-999",
      amount: 149.99,
      currency: "USD",
      userId: "user-123",
    },
    {
      persistent: true,
      correlationId: "txn-abc-456",
      headers: { "x-idempotency-key": "idem-PAY-999" },
    }
  );

  if (confirmed) {
    console.log("✓ Broker confirmed — message is safely queued");
  } else {
    console.error("✗ Broker rejected — implement fallback!");
  }

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
