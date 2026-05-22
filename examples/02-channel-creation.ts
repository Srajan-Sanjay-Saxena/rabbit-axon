/**
 * Example: Channel & Confirm Channel Creation
 *
 * Demonstrates:
 * - Creating a regular channel
 * - Creating a confirm channel (broker acks publishes)
 * - Channels have error/close handlers attached automatically
 */

import { RabbitMqBaseClass } from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");

  // Regular channel — fire-and-forget publishing
  const channel = await rabbit.createChannel();
  console.log("Regular channel created");

  // Confirm channel — broker acknowledges every publish
  const confirmChannel = await rabbit.createConfirmChannel();
  console.log("Confirm channel created");

  confirmChannel.publish("my-exchange", "my-key", Buffer.from("hello"));

  try {
    await confirmChannel.waitForConfirms();
    console.log("Broker confirmed the message");
  } catch {
    console.error("Broker rejected the message");
  }

  await channel.close();
  await confirmChannel.close();
  await rabbit.gracefulShutdown();
}

main().catch(console.error);
