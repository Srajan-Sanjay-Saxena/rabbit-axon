/**
 * Example: Connection & Graceful Shutdown
 *
 * Demonstrates:
 * - Creating a connection with custom options
 * - Heartbeat, reconnect interval, max attempts
 * - Graceful shutdown on SIGTERM/SIGINT
 */

import { RabbitMqBaseClass } from "../src/index.js";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://guest:guest@localhost:5672", {
    heartbeat: 30,
    reconnectInterval: 3000,
    maxReconnectAttempts: 15,
    frameMax: 0,
    channelMax: 100,
  });

  await rabbit.ConnectToService();
  console.log("Connected to RabbitMQ!");

  const shutdown = async () => {
    console.log("Shutting down...");
    await rabbit.gracefulShutdown();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch(console.error);
