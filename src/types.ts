import type amqp from "amqplib";

export type ExchangeTopics = "fanout" | "direct" | "topic" | "headers";

export interface ExchangeOptions {
  durable?: boolean;
  autoDelete?: boolean;
  internal?: boolean;
  alternateExchange?: string;
  arguments?: Record<string, any>;
}

export interface RabbitConnectionOptions {
  heartbeat?: number;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
  frameMax?: number;
  channelMax?: number;
}

export interface QueueArguments {
  "x-dead-letter-exchange"?: string;
  "x-dead-letter-routing-key"?: string;
  "x-message-ttl"?: number;
  "x-max-length"?: number;
  "x-max-priority"?: number;
  "x-queue-mode"?: "default" | "lazy";
  "x-expires"?: number;
  "x-overflow"?: "drop-head" | "reject-publish";
  [key: string]: any;
}

export interface PublishOptions {
  persistent?: boolean;
  priority?: number;
  expiration?: string;
  correlationId?: string;
  replyTo?: string;
  messageId?: string;
  timestamp?: number;
  contentType?: string;
  headers?: Record<string, any>;
}

export interface ConsumeOptions {
  prefetchCount?: number;
  retryLimit?: number;
  dlx?: boolean;
  serializer?: import("./serializer/serializer.js").ISerializer;
}

export type MessageHandler<T extends Record<string, any> | Buffer = Record<string, any>> = (
  data: T,
  msg: amqp.ConsumeMessage
) => Promise<void>;
