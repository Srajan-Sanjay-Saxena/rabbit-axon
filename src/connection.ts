import amqp from "amqplib";
import type { RabbitConnectionOptions } from "./types.js";

export class RabbitMqBaseClass {
  private _rabbitConnection!: amqp.ChannelModel;
  private rabbitConnString: string;
  private options: Required<RabbitConnectionOptions>;
  private reconnectAttempts = 0;
  private isShuttingDown = false;
  private onReconnectCallbacks: Array<() => Promise<void>> = [];

  public get rabbitConnection(): amqp.ChannelModel {
    return this._rabbitConnection;
  }

  public constructor(
    rabbitConnString: string,
    options: RabbitConnectionOptions = {}
  ) {
    this.rabbitConnString = rabbitConnString;
    this.options = {
      heartbeat: options.heartbeat ?? 60,
      reconnectInterval: options.reconnectInterval ?? 5000,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      frameMax: options.frameMax ?? 0,
      channelMax: options.channelMax ?? 0,
    };
  }

  public onReconnect(cb: () => Promise<void>) {
    this.onReconnectCallbacks.push(cb);
  }

  public async ConnectToService() {
    const { reconnectInterval, maxReconnectAttempts, ...amqpOptions } =
      this.options;
    this._rabbitConnection = await amqp.connect(
      this.rabbitConnString,
      amqpOptions
    );

    this.reconnectAttempts = 0;

    this._rabbitConnection.on("error", (err) => {
      console.error("[RabbitMQ] Connection error:", err);
    });

    this._rabbitConnection.on("close", () => {
      console.warn("[RabbitMQ] Connection closed");
      if (!this.isShuttingDown) this.reconnect();
    });
  }

  private async reconnect() {
    if (this.reconnectAttempts >= this.options.maxReconnectAttempts) {
      console.error("[RabbitMQ] Max reconnect attempts reached");
      process.exitCode = 1;
      return;
    }
    this.reconnectAttempts++;
    console.log(
      `[RabbitMQ] Reconnecting (attempt ${this.reconnectAttempts})...`
    );
    await new Promise((r) => setTimeout(r, this.options.reconnectInterval));
    try {
      await this.ConnectToService();
      for (const cb of this.onReconnectCallbacks) {
        await cb();
      }
    } catch {
      await this.reconnect();
    }
  }

  public async createChannel() {
    if (!this._rabbitConnection) await this.ConnectToService();
    const channel = await this._rabbitConnection.createChannel();
    channel.on("error", (err) => {
      console.error("[RabbitMQ] Channel error:", err);
    });
    channel.on("close", () => {
      console.warn("[RabbitMQ] Channel closed");
    });
    return channel;
  }

  public async createConfirmChannel() {
    if (!this._rabbitConnection) await this.ConnectToService();
    const channel = await this._rabbitConnection.createConfirmChannel();
    channel.on("error", (err) => {
      console.error("[RabbitMQ] Confirm channel error:", err);
    });
    return channel;
  }

  public async gracefulShutdown() {
    this.isShuttingDown = true;
    if (this._rabbitConnection) {
      await this._rabbitConnection.close();
      console.log("[RabbitMQ] Connection closed gracefully");
    }
  }
}
