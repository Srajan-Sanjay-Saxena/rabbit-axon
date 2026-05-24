import type amqp from "amqplib";
import type { RabbitMqBaseClass } from "./connection.js";
import type { ExchangeTopics, ExchangeOptions, QueueArguments } from "./types.js";

export abstract class RabbitMqQueueExchange {
  static exchangeName: string;
  static exchangeType: ExchangeTopics;
  static exchangeOptions: ExchangeOptions = {
    durable: true,
    autoDelete: false,
  };

  private rabbitChannel!: amqp.Channel;

  private get config() {
    return this.constructor as typeof RabbitMqQueueExchange;
  }

  protected async startChannelization(rabbitBaseInstance: RabbitMqBaseClass) {
    this.rabbitChannel = await rabbitBaseInstance.createChannel();
  }

  protected async createExchange() {
    await this.rabbitChannel.assertExchange(
      this.config.exchangeName,
      this.config.exchangeType,
      this.config.exchangeOptions
    );
  }

  protected async deleteExchange(ifUnused = false) {
    await this.rabbitChannel.deleteExchange(this.config.exchangeName, { ifUnused });
  }

  protected async createQueue(
    queueName: string,
    bindKey: string = "",
    queueOptions: amqp.Options.AssertQueue = {
      durable: true,
      autoDelete: false,
    },
    args?: QueueArguments,
    headers?: Record<string, string>
  ): Promise<amqp.Replies.AssertQueue> {
    const queue = await this.rabbitChannel.assertQueue(queueName, {
      ...queueOptions,
      arguments: args,
    });
    await this.rabbitChannel.bindQueue(
      queueName,
      this.config.exchangeName,
      bindKey,
      headers
    );
    return queue;
  }

  protected async createDeadLetterQueue(
    queueName: string,
    dlxExchange: string,
    dlxRoutingKey: string = "",
    ttl?: number
  ) {
    const args: QueueArguments = {
      "x-dead-letter-exchange": dlxExchange,
      "x-dead-letter-routing-key": dlxRoutingKey,
    };
    if (ttl) args["x-message-ttl"] = ttl;

    return this.createQueue(queueName, dlxRoutingKey, { durable: true }, args);
  }

  protected async deleteQueue(
    queueName: string,
    ifEmpty = false,
    ifUnused = false
  ) {
    await this.rabbitChannel.deleteQueue(queueName, { ifEmpty, ifUnused });
  }

  protected async purgeQueue(queueName: string) {
    await this.rabbitChannel.purgeQueue(queueName);
  }

  protected async unbindQueue(
    queueName: string,
    bindKey: string = "",
    headers?: Record<string, string>
  ) {
    await this.rabbitChannel.unbindQueue(
      queueName,
      this.config.exchangeName,
      bindKey,
      headers
    );
  }
}
