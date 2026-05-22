/**
 * Example: Full End-to-End (Setup → Produce → Consume → DLX → Shutdown)
 *
 * Demonstrates the complete lifecycle:
 * 1. Create exchange with DLX
 * 2. Publish messages with confirms
 * 3. Consume with multi-worker + retry
 * 4. Failed messages land in DLQ
 * 5. Graceful shutdown
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

// --- 1. Exchange Setup ---

class InvoiceExchange extends RabbitMqQueueExchange {
  constructor() {
    super("invoices", "topic", { durable: true });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // DLQ for permanently failed invoices
    await this.createQueue("invoices.dlq", "invoice.failed.#", {
      durable: true,
    });

    // Main processing queue with DLX
    await this.createDeadLetterQueue(
      "invoices.process",
      "invoices",            // DLX = same exchange
      "invoice.failed",     // DLX routing key
      180000                // 3 min TTL
    );

    // Separate queue for invoice notifications
    await this.createQueue(
      "invoices.notify",
      "invoice.created",
      { durable: true },
      { "x-max-length": 1000 }
    );

    console.log("Invoice exchange setup complete");
  }
}

// --- 2. Producer ---

async function publishInvoices(rabbit: RabbitMqBaseClass) {
  const invoices = [
    { invoiceId: "INV-001", amount: 250.0, customer: "Acme Corp" },
    { invoiceId: "INV-002", amount: 89.99, customer: "Globex Inc" },
    { invoiceId: "INV-003", amount: 1500.0, customer: "Initech LLC" },
  ];

  for (const invoice of invoices) {
    const producer = new RabbitProducerExchanger(
      "invoices",
      invoice,
      "invoice.created"
    );

    const confirmed = await producer.produceWithConfirm(rabbit, {
      persistent: true,
      messageId: `msg-${invoice.invoiceId}`,
      priority: invoice.amount > 1000 ? 9 : 1, // high-value = high priority
    });

    console.log(
      `${invoice.invoiceId}: ${confirmed ? "confirmed ✓" : "REJECTED ✗"}`
    );
  }
}

// --- 3. Consumer ---

async function startConsumers(rabbit: RabbitMqBaseClass) {
  // Process invoices (with retry)
  const processor = new RabbitProducerExchanger("invoices", {});
  await processor.consumeMessage(
    rabbit,
    "invoices.process",
    async (data, msg) => {
      const attempt = (msg.properties.headers?.["x-retry-count"] ?? 0) + 1;
      console.log(
        `Processing ${data.invoiceId} ($${data.amount}) [attempt ${attempt}]`
      );

      // Simulate: large invoices need approval and sometimes fail
      if (data.amount > 1000 && attempt < 3) {
        throw new Error("Approval service unavailable");
      }

      console.log(`  ${data.invoiceId} processed ✓`);
    },
    {
      workerCount: 3,
      prefetchCount: 1,
      requeueOnFailure: true,
      retryLimit: 3,
    }
  );

  // Notify about new invoices (simple, no retry)
  const notifier = new RabbitProducerExchanger("invoices", {});
  await notifier.consumeMessage(
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

  // Setup
  const exchange = new InvoiceExchange();
  await exchange.setup(rabbit);

  // Start consumers
  await startConsumers(rabbit);
  console.log("\nConsumers running. Publishing invoices...\n");

  // Publish
  await publishInvoices(rabbit);

  // Graceful shutdown on signal
  process.on("SIGINT", async () => {
    console.log("\nShutting down gracefully...");
    await rabbit.gracefulShutdown();
    process.exit(0);
  });
}

main().catch(console.error);
