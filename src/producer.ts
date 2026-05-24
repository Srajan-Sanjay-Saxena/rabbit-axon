import type amqp from "amqplib";
import type { RabbitMqBaseClass } from "./connection.js";
import type { PublishOptions } from "./types.js";

export class RabbitProducer {
  private exchangeName: string;
  private routingKey: string;

  public constructor(exchangeName: string, routingKey: string = "") {
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
  }

  private buildPublishOpts(options: PublishOptions): amqp.Options.Publish {
    return {
      persistent: options.persistent ?? true,
      priority: options.priority,
      expiration: options.expiration,
      correlationId: options.correlationId,
      replyTo: options.replyTo,
      messageId: options.messageId,
      timestamp: options.timestamp ?? Date.now(),
      contentType: options.contentType ?? "application/json",
      headers: options.headers,
    };
  }

  public async publish(
    rabbitBaseInstance: RabbitMqBaseClass,
    data: Record<string, any>,
    options: PublishOptions = {}
  ) {
    const channel = await rabbitBaseInstance.createChannel();
    try {
      channel.publish(
        this.exchangeName,
        this.routingKey,
        Buffer.from(JSON.stringify(data)),
        this.buildPublishOpts(options)
      );
    } finally {
      await channel.close();
    }
  }

  public async publishWithConfirm(
    rabbitBaseInstance: RabbitMqBaseClass,
    data: Record<string, any>,
    options: PublishOptions = {}
  ): Promise<boolean> {
    const channel = await rabbitBaseInstance.createConfirmChannel();

    channel.publish(
      this.exchangeName,
      this.routingKey,
      Buffer.from(JSON.stringify(data)),
      this.buildPublishOpts(options)
    );

    try {
      await channel.waitForConfirms();
      await channel.close();
      return true;
    } catch {
      await channel.close();
      return false;
    }
  }
}
