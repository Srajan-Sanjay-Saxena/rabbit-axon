import type amqp from "amqplib";
import { ChannelManager } from "../channel/manager.js";
import { RabbitLogger } from "../logger/logger.js";
import type { ExchangeTopics, ExchangeOptions, QueueArguments } from "../types.js";
import type { IRabbitConnection } from "../connection/single.js";

export abstract class RabbitMqQueueExchange {
  private exchangeName: string;
  private exchangeType: ExchangeTopics;
  private exchangeOptions: ExchangeOptions;
  private connInstance: IRabbitConnection;
  protected logger: RabbitLogger;

  public constructor(
    connInstance: IRabbitConnection,
    exchangeName: string,
    exchangeType: ExchangeTopics,
    exchangeOptions: ExchangeOptions = { durable: true, autoDelete: false },
  ) {
    this.connInstance = connInstance;
    this.exchangeName = exchangeName;
    this.exchangeType = exchangeType;
    this.exchangeOptions = exchangeOptions;
    this.logger = new RabbitLogger();
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
  }

  private async withChannel<T>(fn: (ch: amqp.Channel) => Promise<T>): Promise<T> {
    const conn = this.connInstance.rabbitConnection;
    if (!conn) {
      this.logger.error("Cannot perform operation — no active connection", "Exchange", { exchange: this.exchangeName });
      throw new Error("[Exchange] No active connection");
    }
    const channel = await ChannelManager.createChannel(conn, () => {
      this.logger.warn("Exchange channel closed", "Exchange", { exchange: this.exchangeName });
    });
    try {
      return await fn(channel);
    } finally {
      await channel.close();
    }
  }

  protected async createExchange() {
    this.logger.info("Asserting exchange", "Exchange", { exchange: this.exchangeName, type: this.exchangeType });
    await this.withChannel((ch) =>
      ch.assertExchange(this.exchangeName, this.exchangeType, this.exchangeOptions)
    );
    this.logger.info("Exchange asserted", "Exchange", { exchange: this.exchangeName });
  }

  protected async deleteExchange(ifUnused = false) {
    this.logger.info("Deleting exchange", "Exchange", { exchange: this.exchangeName, ifUnused });
    await this.withChannel((ch) =>
      ch.deleteExchange(this.exchangeName, { ifUnused })
    );
    this.logger.info("Exchange deleted", "Exchange", { exchange: this.exchangeName });
  }

  protected async createQueue(
    queueName: string,
    bindKey: string = "",
    queueOptions: amqp.Options.AssertQueue = { durable: true, autoDelete: false },
    args?: QueueArguments,
    headers?: Record<string, string>,
  ): Promise<amqp.Replies.AssertQueue> {
    this.logger.info("Asserting queue", "Exchange", { queue: queueName, bindKey, exchange: this.exchangeName });
    const result = await this.withChannel(async (ch) => {
      const queue = await ch.assertQueue(queueName, { ...queueOptions, arguments: args });
      await ch.bindQueue(queueName, this.exchangeName, bindKey, headers);
      return queue;
    });
    this.logger.info("Queue asserted and bound", "Exchange", { queue: queueName, bindKey });
    return result;
  }

  protected async createDeadLetterQueue(
    queueName: string,
    dlxExchange: string,
    dlxRoutingKey: string = "",
    ttl?: number,
  ) {
    this.logger.info("Asserting dead letter queue", "Exchange", { queue: queueName, dlxExchange, dlxRoutingKey, ttl });
    const args: QueueArguments = {
      "x-dead-letter-exchange": dlxExchange,
      "x-dead-letter-routing-key": dlxRoutingKey,
    };
    if (ttl) args["x-message-ttl"] = ttl;
    return this.createQueue(queueName, dlxRoutingKey, { durable: true }, args);
  }

  protected async deleteQueue(queueName: string, ifEmpty = false, ifUnused = false) {
    this.logger.info("Deleting queue", "Exchange", { queue: queueName, ifEmpty, ifUnused });
    await this.withChannel((ch) =>
      ch.deleteQueue(queueName, { ifEmpty, ifUnused })
    );
    this.logger.info("Queue deleted", "Exchange", { queue: queueName });
  }

  protected async purgeQueue(queueName: string) {
    this.logger.info("Purging queue", "Exchange", { queue: queueName });
    await this.withChannel((ch) => ch.purgeQueue(queueName));
    this.logger.info("Queue purged", "Exchange", { queue: queueName });
  }

  protected async unbindQueue(queueName: string, bindKey: string = "", headers?: Record<string, string>) {
    this.logger.info("Unbinding queue", "Exchange", { queue: queueName, bindKey, exchange: this.exchangeName });
    await this.withChannel((ch) =>
      ch.unbindQueue(queueName, this.exchangeName, bindKey, headers)
    );
    this.logger.info("Queue unbound", "Exchange", { queue: queueName, bindKey });
  }
}
