import type amqp from "amqplib";
import { RabbitLogger } from "../logger/logger.js";
import type { IRabbitConnection } from "../connection/single.js";
import type { ConsumeOptions, MessageHandler } from "../types.js";

export class RabbitConsumer<T extends Record<string, any> = Record<string, any>> {
  private connInstance: IRabbitConnection;
  private logger: RabbitLogger;

  public constructor(connInstance: IRabbitConnection) {
    this.connInstance = connInstance;
    this.logger = new RabbitLogger();
    connInstance.onReconnect(async () => {
      this.logger.info("Reconnected — channel will be recreated on next consume", "Consumer");
    });
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
  }

  private async getChannel(): Promise<amqp.Channel> {
    try {
      return await this.connInstance.getChannel();
    } catch (err) {
      this.logger.error("Cannot get channel — no active connection", "Consumer");
      throw err;
    }
  }

  private async retryWithBackoff(
    handler: MessageHandler<T>,
    data: T,
    msg: amqp.ConsumeMessage,
    retryLimit: number,
    queue: string,
  ): Promise<void> {
    let attempt = 0;
    while (attempt <= retryLimit) {
      try {
        await handler(data, msg);
        return;
      } catch (err) {
        if (attempt === retryLimit) throw err;
        const delay = Math.min(1000 * 2 ** attempt, 30000);
        this.logger.warn("Retrying message", "Consumer", { attempt: attempt + 1, retryLimit, delay, queue });
        await new Promise((r) => setTimeout(r, delay));
        attempt++;
      }
    }
  }

  public async consume(
    queueName: string,
    handler: MessageHandler<T>,
    options: ConsumeOptions = {}
  ) {
    const { prefetchCount = 1, retryLimit = 3, dlx = false } = options;
    this.logger.info("Setting up consumer", "Consumer", { queue: queueName, prefetchCount, dlx });

    const startConsuming = async () => {
      const channel = await this.getChannel();
      await channel.prefetch(prefetchCount);

      channel.consume(
        queueName,
        async (msg) => {
          if (msg === null) return;
          const data = JSON.parse(msg.content.toString()) as T;

          if (dlx) {
            try {
              await handler(data, msg);
              channel.ack(msg);
            } catch (err) {
              this.logger.error("Processing failed, routing to DLX", "Consumer", { queue: queueName, err });
              channel.nack(msg, false, false);
            }
          } else {
            try {
              await this.retryWithBackoff(handler, data, msg, retryLimit, queueName);
              channel.ack(msg);
            } catch (err) {
              this.logger.error("Message failed after all retries, skipping", "Consumer", { queue: queueName, retryLimit, err });
              channel.ack(msg);
            }
          }
        },
        { noAck: false }
      );

      this.logger.info("Consumer started", "Consumer", { queue: queueName, prefetch: prefetchCount, dlx });
    };

    await startConsuming();

    this.connInstance.onReconnect(async () => {
      this.logger.info("Re-establishing consumer after reconnect", "Consumer", { queue: queueName });
      await startConsuming();
    });
  }
}
