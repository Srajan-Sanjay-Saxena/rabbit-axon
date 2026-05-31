import { encode, decode } from "@msgpack/msgpack";

export interface ISerializer {
  serialize(data: unknown): Buffer;
  deserialize(buffer: Buffer): unknown;
  readonly contentType: string;
}

export class JsonSerializer implements ISerializer {
  readonly contentType = "application/json";

  serialize(data: unknown): Buffer {
    return Buffer.from(JSON.stringify(data));
  }

  deserialize(buffer: Buffer): unknown {
    return JSON.parse(buffer.toString());
  }
}

export class MsgpackSerializer implements ISerializer {
  readonly contentType = "application/msgpack";

  serialize(data: unknown): Buffer {
    return Buffer.from(encode(data));
  }

  deserialize(buffer: Buffer): unknown {
    return decode(buffer);
  }
}

export class IdentitySerializer implements ISerializer {
  readonly contentType = "application/octet-stream";

  serialize(data: unknown): Buffer {
    if (!Buffer.isBuffer(data))
      throw new Error("[IdentitySerializer] data must be a Buffer — encode it yourself before passing");
    return data;
  }

  deserialize(buffer: Buffer): unknown {
    return buffer;
  }
}

export const defaultSerializer = new JsonSerializer();
