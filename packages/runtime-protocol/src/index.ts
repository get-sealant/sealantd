// @sealant/runtime-protocol
//
// TypeScript view of the sealantd control protocol over the Protobuf wire (ADR-0012). The schema is
// loaded at runtime from the Rust crate's sealant.proto (the single source of truth; vendored into
// this package at monorepo integration). Length-prefixed framing is unchanged.
//
// Message objects use protobuf.js's shape: camelCase fields, oneofs as a discriminator field plus
// the active case's data (e.g. a ServerMessage event sets `event`; an io.chunk event sets `ioChunk`
// and `payload === "ioChunk"`). Binary fields decode to Buffer (no base64).

import protobuf from "protobufjs";
import { Buffer } from "node:buffer";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = resolve(here, "../../../crates/sealant-protocol/proto/sealant.proto");

const root = protobuf.loadSync(PROTO_PATH);
const ClientMessageT = root.lookupType("sealant.v1.ClientMessage");
const ServerMessageT = root.lookupType("sealant.v1.ServerMessage");

/** Current wire schema version. */
export const SCHEMA_VERSION = 1;
/** Default maximum control-frame body size (8 MiB). */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;

/** A protobuf command oneof, e.g. `{ runtimeHealth: {} }` or `{ exec: { executable, args } }`. */
export type Command = Record<string, unknown>;

/** A control request payload (the `request` case of ClientMessage). */
export interface ControlRequest {
  schemaVersion: number;
  requestId: string;
  command: Command;
}

/** A decoded server message: `{ message, response?|event? }` (protobuf.js oneof shape). */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ServerMessage = Record<string, any>;

/** Encode a client request to protobuf bytes. */
export function encodeClient(request: ControlRequest): Buffer {
  const payload = { request };
  const err = ClientMessageT.verify(payload);
  if (err) {
    throw new Error(`invalid client message: ${err}`);
  }
  return Buffer.from(ClientMessageT.encode(ClientMessageT.create(payload)).finish());
}

/** Decode a server message from protobuf bytes. */
export function decodeServer(bytes: Buffer): ServerMessage {
  const message = ServerMessageT.decode(bytes);
  return ServerMessageT.toObject(message, {
    enums: String,
    longs: Number,
    bytes: Buffer,
    defaults: false,
    oneofs: true,
  });
}

/** Frame a protobuf body with a big-endian u32 length prefix. */
export function encodeFrame(body: Buffer): Buffer {
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

/** Incremental frame decoder: feed socket chunks, get decoded ServerMessages back. */
export class FrameDecoder {
  #buffer: Buffer = Buffer.alloc(0);
  #maxFrameBytes: number;

  constructor(maxFrameBytes: number = DEFAULT_MAX_FRAME_BYTES) {
    this.#maxFrameBytes = maxFrameBytes;
  }

  push(chunk: Buffer): ServerMessage[] {
    this.#buffer = this.#buffer.length === 0 ? chunk : Buffer.concat([this.#buffer, chunk]);
    const messages: ServerMessage[] = [];
    while (this.#buffer.length >= 4) {
      const length = this.#buffer.readUInt32BE(0);
      if (length > this.#maxFrameBytes) {
        throw new Error(`frame length ${length} exceeds maximum ${this.#maxFrameBytes}`);
      }
      if (this.#buffer.length < 4 + length) {
        break;
      }
      messages.push(decodeServer(this.#buffer.subarray(4, 4 + length)));
      this.#buffer = this.#buffer.subarray(4 + length);
    }
    return messages;
  }
}

// --- helpers over the protobuf.js shape ---

/** Whether a server message is a telemetry event. */
export function isEvent(message: ServerMessage): boolean {
  return message.event !== undefined;
}

/** Whether a server message is a response. */
export function isResponse(message: ServerMessage): boolean {
  return message.response !== undefined;
}

/** Whether an event is an `io.chunk`. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isIoChunk(event: any): boolean {
  return event?.ioChunk !== undefined;
}

/** Whether an event is a `process.exited`. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isProcessExited(event: any): boolean {
  return event?.processExited !== undefined;
}

/** Raw bytes of an `io.chunk` event (empty if none). */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function chunkBytes(event: any): Buffer {
  const content = event?.ioChunk?.content;
  return content ? Buffer.from(content) : Buffer.alloc(0);
}

/** Proto enum string for stdout. */
export const STREAM_STDOUT = "STREAM_KIND_STDOUT";
