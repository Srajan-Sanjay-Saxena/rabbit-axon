/**
 * Example: RPC Pattern (Request-Response)
 *
 * Demonstrates:
 * - correlationId: match response to request
 * - replyTo: tell the server where to send the response
 * - Useful for synchronous-style communication over async messaging
 */

import amqp from "amqplib";
import {
  RabbitMqBaseClass,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

async function rpcClient() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  const channel = await rabbit.createChannel();

  // Create exclusive reply queue (auto-delete, unique per client)
  const { queue: replyQueue } = await channel.assertQueue("", {
    exclusive: true,
  });

  const correlationId = `rpc-${Date.now()}-${Math.random()}`;

  // Send RPC request
  const producer = new RabbitProducerExchanger(
    "rpc.exchange",
    { action: "getUser", userId: "user-42" },
    "rpc.request"
  );

  await producer.produceMessage(rabbit, {
    correlationId,
    replyTo: replyQueue,
  });

  console.log(`[Client] Sent RPC request (correlationId: ${correlationId})`);

  // Wait for response
  return new Promise<Record<string, any>>((resolve) => {
    channel.consume(
      replyQueue,
      (msg) => {
        if (msg && msg.properties.correlationId === correlationId) {
          const response = JSON.parse(msg.content.toString());
          console.log("[Client] Got response:", response);
          channel.ack(msg);
          resolve(response);
        }
      },
      { noAck: false }
    );
  });
}

async function rpcServer() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const consumer = new RabbitProducerExchanger("rpc.exchange", {});

  await consumer.consumeMessage(
    rabbit,
    "rpc.requests",
    async (data, msg) => {
      console.log("[Server] Processing RPC:", data.action);

      // Process the request
      const result = { name: "John Doe", id: data.userId, active: true };

      // Send response back to replyTo queue
      const channel = await rabbit.createChannel();
      channel.sendToQueue(
        msg.properties.replyTo,
        Buffer.from(JSON.stringify(result)),
        { correlationId: msg.properties.correlationId }
      );
      await channel.close();

      console.log("[Server] Response sent");
    },
    { workerCount: 2 }
  );

  console.log("[Server] RPC server running...");
}

// Run both
async function main() {
  await rpcServer();
  const response = await rpcClient();
  console.log("Final response:", response);
}

main().catch(console.error);
