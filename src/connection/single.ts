import amqp from "amqplib";
import { ChannelManager } from "../channel/manager.js";
import { RabbitLogger } from "../logger/logger.js";
import type { RabbitConnectionOptions } from "../types.js";

export interface IRabbitConnection {
  ConnectToService(): Promise<void>;
  onReconnect(cb: () => Promise<void>): void;
  gracefulShutdown(): Promise<void>;
  addLogger(logger: RabbitLogger): void;
  get rabbitConnection(): amqp.ChannelModel | null;
  getChannel(): Promise<amqp.Channel>;
  getConfirmChannel(): Promise<amqp.ConfirmChannel>;
}

export class RabbitSingleConnectionHandler implements IRabbitConnection {
  private _rabbitConnection: amqp.ChannelModel | null = null;
  private _channel: amqp.Channel | null = null;
  private _confirmChannel: amqp.ConfirmChannel | null = null;
  private rabbitConnString: string;
  private options: Required<RabbitConnectionOptions>;
  private reconnectAttempts = 0;
  private isShuttingDown = false;
  private onReconnectCallbacks: Array<() => Promise<void>> = [];
  private logger: RabbitLogger;

  public get rabbitConnection(): amqp.ChannelModel | null {
    return this._rabbitConnection;
  }

  public constructor(rabbitConnString: string, options: RabbitConnectionOptions = {}) {
    this.rabbitConnString = rabbitConnString;
    this.options = {
      heartbeat: options.heartbeat ?? 60,
      reconnectInterval: options.reconnectInterval ?? 5000,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      frameMax: options.frameMax ?? 0,
      channelMax: options.channelMax ?? 0,
    };
    this.logger = new RabbitLogger();
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
  }

  public onReconnect(cb: () => Promise<void>) {
    this.onReconnectCallbacks.push(cb);
  }

  public async ConnectToService() {
    const { reconnectInterval, maxReconnectAttempts, ...amqpOptions } = this.options;
    this.logger.info("Connecting to RabbitMQ", "SingleConnection");
    this._rabbitConnection = await amqp.connect(this.rabbitConnString, amqpOptions);
    this.reconnectAttempts = 0;
    this.logger.info("Connected to RabbitMQ", "SingleConnection");

    this._rabbitConnection.on("error", (err) => {
      this.logger.error("Connection error", "SingleConnection", { err });
      this._rabbitConnection = null;
    });
    this._rabbitConnection.on("close", () => {
      this.logger.warn("Connection closed", "SingleConnection");
      this._rabbitConnection = null;
      this._channel = null;
      this._confirmChannel = null;
      if (!this.isShuttingDown) this.Reconnect();
    });
  }

  public async getChannel(): Promise<amqp.Channel> {
    if (!this._rabbitConnection) throw new Error("[SingleConnection] No active connection");
    if (!this._channel) {
      this._channel = await ChannelManager.createChannel(this._rabbitConnection, () => {
        this.logger.warn("Channel closed", "SingleConnection");
        this._channel = null;
      });
      this.logger.debug("Channel created", "SingleConnection");
    }
    return this._channel;
  }

  public async getConfirmChannel(): Promise<amqp.ConfirmChannel> {
    if (!this._rabbitConnection) throw new Error("[SingleConnection] No active connection");
    if (!this._confirmChannel) {
      this._confirmChannel = await ChannelManager.createConfirmChannel(this._rabbitConnection, () => {
        this.logger.warn("Confirm channel closed", "SingleConnection");
        this._confirmChannel = null;
      });
      this.logger.debug("Confirm channel created", "SingleConnection");
    }
    return this._confirmChannel;
  }

  private async Reconnect() {
    if (this.reconnectAttempts >= this.options.maxReconnectAttempts) {
      this.logger.error("Max reconnect attempts reached, giving up", "SingleConnection", {
        maxAttempts: this.options.maxReconnectAttempts,
      });
      process.exitCode = 1;
      return;
    }
    this.reconnectAttempts++;
    this.logger.info("Attempting reconnect", "SingleConnection", {
      attempt: this.reconnectAttempts,
      maxAttempts: this.options.maxReconnectAttempts,
      retryInMs: this.options.reconnectInterval,
    });
    await new Promise((r) => setTimeout(r, this.options.reconnectInterval));
    const currentAttempt = this.reconnectAttempts;
    try {
      await this.ConnectToService();
      this.logger.info("Reconnected successfully", "SingleConnection", { attempt: currentAttempt });
      for (const cb of this.onReconnectCallbacks) await cb();
    } catch (err) {
      this.logger.error("Reconnect attempt failed", "SingleConnection", { attempt: currentAttempt, err });
      await this.Reconnect();
    }
  }

  public async gracefulShutdown() {
    this.logger.info("Initiating graceful shutdown", "SingleConnection");
    this.isShuttingDown = true;
    this._channel = null;
    this._confirmChannel = null;
    if (this._rabbitConnection) {
      await this._rabbitConnection.close();
      this._rabbitConnection = null;
      this.logger.info("Connection closed gracefully", "SingleConnection");
    } else {
      this.logger.warn("Graceful shutdown called but no active connection", "SingleConnection");
    }
    this.isShuttingDown = false;
    this.reconnectAttempts = 0;
  }
}
