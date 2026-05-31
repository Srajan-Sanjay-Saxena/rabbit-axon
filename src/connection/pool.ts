import type amqp from "amqplib";
import { type IRabbitConnection, RabbitSingleConnectionHandler } from "./single.js";
import { ChannelManager } from "../channel/manager.js";
import { RabbitLogger } from "../logger/logger.js";
import type { RabbitConnectionOptions } from "../types.js";

export class RabbitConnectionPoolHandler implements IRabbitConnection {
  private connections: RabbitSingleConnectionHandler[] = [];
  private channels: Map<RabbitSingleConnectionHandler, amqp.Channel> = new Map();
  private confirmChannels: Map<RabbitSingleConnectionHandler, amqp.ConfirmChannel> = new Map();
  private roundRobinIndex = 0;
  private poolSize: number;
  private connString: string;
  private options: RabbitConnectionOptions;
  private logger: RabbitLogger;

  public constructor(connString: string, poolSize: number = 3, options: RabbitConnectionOptions = {}) {
    this.connString = connString;
    this.poolSize = poolSize;
    this.options = options;
    this.logger = new RabbitLogger();
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
    for (const conn of this.connections) conn.addLogger(logger);
  }

  public async ConnectToService() {
    this.logger.info("Initializing connection pool", "ConnectionPool", { size: this.poolSize });
    for (let i = 0; i < this.poolSize; i++) {
      const conn = new RabbitSingleConnectionHandler(this.connString, this.options);
      conn.addLogger(this.logger);
      await conn.ConnectToService();
      this.connections.push(conn);

      // when a connection drops, invalidate its channels from the maps
      conn.onReconnect(async () => {
        this.channels.delete(conn);
        this.confirmChannels.delete(conn);
        this.logger.debug("Invalidated channels for reconnected connection", "ConnectionPool", { index: i });
      });

      this.logger.debug("Connection added to pool", "ConnectionPool", { index: i });
    }
    this.logger.info("Connection pool ready", "ConnectionPool", { size: this.poolSize });
  }

  // internal — round-robins across active connections
  private acquire(): RabbitSingleConnectionHandler {
    if (this.connections.length === 0)
      throw new Error("[ConnectionPool] No connections available");
    const total = this.connections.length;
    for (let i = 0; i < total; i++) {
      const index = this.roundRobinIndex % total;
      this.roundRobinIndex++;
      const conn = this.connections[index];
      if (conn.rabbitConnection) return conn;
      this.logger.warn("Skipping dropped connection during round-robin", "ConnectionPool", { index });
    }
    throw new Error("[ConnectionPool] No active connections available");
  }

  public get rabbitConnection(): amqp.ChannelModel | null {
    try {
      return this.acquire().rabbitConnection;
    } catch {
      return null;
    }
  }

  public async getChannel(): Promise<amqp.Channel> {
    const conn = this.acquire();
    const existing = this.channels.get(conn);
    if (existing) return existing;

    const channel = await ChannelManager.createChannel(conn.rabbitConnection!, () => {
      this.channels.delete(conn);
      this.logger.warn("Pool channel closed", "ConnectionPool");
    });
    this.channels.set(conn, channel);
    this.logger.debug("Pool channel created", "ConnectionPool");
    return channel;
  }

  public async getConfirmChannel(): Promise<amqp.ConfirmChannel> {
    const conn = this.acquire();
    const existing = this.confirmChannels.get(conn);
    if (existing) return existing;

    const channel = await ChannelManager.createConfirmChannel(conn.rabbitConnection!, () => {
      this.confirmChannels.delete(conn);
      this.logger.warn("Pool confirm channel closed", "ConnectionPool");
    });
    this.confirmChannels.set(conn, channel);
    this.logger.debug("Pool confirm channel created", "ConnectionPool");
    return channel;
  }

  public onReconnect(cb: () => Promise<void>) {
    for (const conn of this.connections) conn.onReconnect(cb);
  }

  public async gracefulShutdown() {
    this.logger.info("Shutting down connection pool", "ConnectionPool", { size: this.connections.length });
    this.channels.clear();
    this.confirmChannels.clear();
    for (const conn of this.connections) await conn.gracefulShutdown();
    this.connections = [];
    this.roundRobinIndex = 0;
    this.logger.info("Connection pool shut down", "ConnectionPool");
  }
}
