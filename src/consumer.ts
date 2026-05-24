import type amqp from "amqplib";
import type { RabbitMqBaseClass } from "./connection.js";
import type { ConsumeOptions, MessageHandler } from "./types.js";

export class RabbitConsumer {
  private exchangeName: string;

  public constructor(exchangeName: string) {
    this.exchangeName = exchangeName;
  }

  public async consume(
    rabbitBaseInstance: RabbitMqBaseClass,
    queueName: string,
    handler: MessageHandler,
    options: ConsumeOptions = {}
  ) {
    const {
      workerCount = 1,
      prefetchCount = 1,
      requeueOnFailure = false,
      retryLimit = 3,
    } = options;

    const startWorkers = async () => {
      for (let i = 0; i < workerCount; i++) {
        const channel = await rabbitBaseInstance.createChannel();
        await channel.prefetch(prefetchCount);

        channel.consume(
          queueName,
          async (msg) => {
            if (msg === null) return;

            const retryCount =
              (msg.properties.headers?.["x-retry-count"] as number) ?? 0;

            try {
              const json = JSON.parse(msg.content.toString());
              await handler(json, msg);
              channel.ack(msg);
            } catch (err) {
              console.error(`[Worker ${i}] Processing failed:`, err);

              try {
                if (requeueOnFailure && retryCount < retryLimit) {
                  channel.publish("", queueName, msg.content, {
                    ...msg.properties,
                    headers: {
                      ...msg.properties.headers,
                      "x-retry-count": retryCount + 1,
                    },
                    expiration: String(
                      Math.min(1000 * 2 ** retryCount, 30000)
                    ),
                  });
                  channel.ack(msg);
                } else {
                  channel.nack(msg, false, false);
                }
              } catch (ackErr) {
                console.error(
                  `[Worker ${i}] Channel operation failed:`,
                  ackErr
                );
              }
            }
          },
          { noAck: false }
        );

        console.log(
          `[Worker ${i}] consuming from ${queueName} (prefetch: ${prefetchCount})`
        );
      }
    };

    await startWorkers();

    rabbitBaseInstance.onReconnect(async () => {
      console.log(`[RabbitMQ] Re-establishing consumers for ${queueName}`);
      await startWorkers();
    });
  }
}
