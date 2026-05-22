/**
 * Example: Auto-Reconnection with Callbacks
 *
 * Demonstrates:
 * - onReconnect() to re-establish logic after connection drops
 * - Consumers/setup automatically re-run after reconnection
 */

import { RabbitMqBaseClass } from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost", {
    reconnectInterval: 2000,
    maxReconnectAttempts: 5,
  });

  await rabbit.ConnectToService();

  // Register a callback that fires after every successful reconnection
  rabbit.onReconnect(async () => {
    console.log("Reconnected! Re-initializing resources...");
    // Re-assert exchanges, re-bind queues, re-start consumers here
  });

  console.log("Service running. Kill RabbitMQ to test reconnection...");

  // Keep process alive
  setInterval(() => {}, 60000);
}

main().catch(console.error);
