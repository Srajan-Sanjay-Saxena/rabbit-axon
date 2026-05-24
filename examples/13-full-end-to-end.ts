/**
 * Example: Full End-to-End (Setup → Produce → Consume → DLX → Shutdown)
 *
 * Demonstrates the complete lifecycle:
 * 1. Create exchange with DLX (static exchangeName)
 * 2. Publish messages with confirms
 * 3. Consume with multi-worker + retry
 * 4. Failed messages land in DLQ
 * 5. Graceful shutdown
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
  RabbitConsumer,
} from "../src/index.js";

// --- 1. Exchange Setup ---

class InvoiceExchange extends RabbitMqQueueExchange {
  static exchangeName = "invoices";
  static exchangeType = "topic" as const;
  static exchangeOptions = { durable: true, autoDelete: false };

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    await this.createQueue("invoices.dlq", "invoice.failed.#", { durable: true });

    await this.createDeadLetterQueue(
      "invoices.process",
      InvoiceExchange.exchangeName,
      "invoice.failed",
      180000
    );

    await this.createQueue(
      "invoices.notify",
      "invoice.created",
      { durable: true },
      { "x-max-length": 1000 }
    );

    console.log(`"${InvoiceExchange.exchangeName}" exchange setup complete`);
  }
}

// --- 2. Producer ---

async function publishInvoices(rabbit: RabbitMqBaseClass) {
  const producer = new RabbitProducer(InvoiceExchange.exchangeName, "invoice.created");

  const invoices = [
    { invoiceId: "INV-001", amount: 250.0, customer: "Acme Corp" },
    { invoiceId: "INV-002", amount: 89.99, customer: "Globex Inc" },
    { invoiceId: "INV-003", amount: 1500.0, customer: "Initech LLC" },
  ];

  for (const invoice of invoices) {
    const confirmed = await producer.publishWithConfirm(rabbit, invoice, {
      persistent: true,
      messageId: `msg-${invoice.invoiceId}`,
      priority: invoice.amount > 1000 ? 9 : 1,
    });

    console.log(
      `${invoice.invoiceId}: ${confirmed ? "confirmed ✓" : "REJECTED ✗"}`
    );
  }
}

// --- 3. Consumers ---

async function startConsumers(rabbit: RabbitMqBaseClass) {
  const processor = new RabbitConsumer(InvoiceExchange.exchangeName);
  await processor.consume(
    rabbit,
    "invoices.process",
    async (data, msg) => {
      const attempt = (msg.properties.headers?.["x-retry-count"] ?? 0) + 1;
      console.log(
        `Processing ${data.invoiceId} ($${data.amount}) [attempt ${attempt}]`
      );

      if (data.amount > 1000 && attempt < 3) {
        throw new Error("Approval service unavailable");
      }

      console.log(`  ${data.invoiceId} processed ✓`);
    },
    { workerCount: 3, prefetchCount: 1, requeueOnFailure: true, retryLimit: 3 }
  );

  const notifier = new RabbitConsumer(InvoiceExchange.exchangeName);
  await notifier.consume(
    rabbit,
    "invoices.notify",
    async (data) => {
      console.log(`  📧 Notification: New invoice ${data.invoiceId} for ${data.customer}`);
    },
    { workerCount: 1 }
  );
}

// --- 4. Main ---

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost", {
    heartbeat: 30,
    reconnectInterval: 3000,
    maxReconnectAttempts: 10,
  });

  await rabbit.ConnectToService();

  const exchange = new InvoiceExchange();
  await exchange.setup(rabbit);

  await startConsumers(rabbit);
  console.log("\nConsumers running. Publishing invoices...\n");

  await publishInvoices(rabbit);

  process.on("SIGINT", async () => {
    console.log("\nShutting down gracefully...");
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

main().catch(console.error);
