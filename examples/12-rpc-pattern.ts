/**
 * Example: RPC Pattern (Request-Response)
 *
 * Demonstrates:
 * - correlationId: match response to request
 * - replyTo: tell the server where to send the response
 * - Static exchangeName for both producer and consumer
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducer,
  RabbitConsumer,
} from "../src/index.js";

class RpcExchange extends RabbitMqQueueExchange {
  static exchangeName = "rpc.exchange";
  static exchangeType = "direct" as const;
}

async function rpcClient(rabbit: RabbitMqBaseClass) {
  const channel = await rabbit.createChannel();

  const { queue: replyQueue } = await channel.assertQueue("", {
    exclusive: true,
  });

  const correlationId = `rpc-${Date.now()}-${Math.random()}`;

  const producer = new RabbitProducer(RpcExchange.exchangeName, "rpc.request");
  await producer.publish(
    rabbit,
    { action: "getUser", userId: "user-42" },
    { correlationId, replyTo: replyQueue }
  );

  console.log(`[Client] Sent RPC request (correlationId: ${correlationId})`);

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

async function rpcServer(rabbit: RabbitMqBaseClass) {
  const consumer = new RabbitConsumer(RpcExchange.exchangeName);

  await consumer.consume(
    rabbit,
    "rpc.requests",
    async (data, msg) => {
      console.log("[Server] Processing RPC:", data.action);

      const result = { name: "John Doe", id: data.userId, active: true };

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

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  await rpcServer(rabbit);
  const response = await rpcClient(rabbit);
  console.log("Final response:", response);

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
