// @sealant/runtime-protocol
//
// Typed wire codec + length-prefixed framing for the sealantd control protocol (ADR-0012). Types are
// generated from `sealant.proto` by Buf (protobuf-es); the schema is baked into the generated code,
// so there is no runtime `.proto` load and the package is self-contained.
//
// Messages are protobuf-es objects: camelCase fields, oneofs as discriminated unions
// (`message.case === "event"`, `payload.case === "ioChunk"`), enums as TS enums (`StreamKind.STDOUT`),
// and `bytes` fields as `Uint8Array` (no base64).

import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { Buffer } from "node:buffer";

import {
  ClientMessageSchema,
  ServerMessageSchema,
  EventEnvelopeSchema,
  type ClientMessage,
  type ServerMessage,
  type EventEnvelope,
} from "./gen/sealant_pb.js";

// Re-export the full generated surface (types, enums, schemas) plus protobuf-es `create`.
export * from "./gen/sealant_pb.js";
export { create } from "@bufbuild/protobuf";

/** Current wire schema version. */
export const SCHEMA_VERSION = 1;
/** Default maximum control-frame body size (8 MiB). */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;

/** Encode a client message to protobuf bytes. */
export function encodeClient(message: ClientMessage): Uint8Array {
  return toBinary(ClientMessageSchema, message);
}

/** Decode a server message from protobuf bytes. */
export function decodeServer(bytes: Uint8Array): ServerMessage {
  return fromBinary(ServerMessageSchema, bytes);
}

/** Decode a single telemetry event from protobuf bytes (e.g. a spooled record). */
export function decodeEvent(bytes: Uint8Array): EventEnvelope {
  return fromBinary(EventEnvelopeSchema, bytes);
}

/** Frame a protobuf body with a big-endian u32 length prefix. */
export function encodeFrame(body: Uint8Array): Buffer {
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, Buffer.from(body)]);
}

/** Incremental frame decoder: feed socket chunks, get decoded `ServerMessage`s back. */
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

/** Narrow a server message to its response (or `undefined`). */
export function asResponse(message: ServerMessage) {
  return message.message.case === "response" ? message.message.value : undefined;
}

/** Narrow a server message to its telemetry event (or `undefined`). */
export function asEvent(message: ServerMessage): EventEnvelope | undefined {
  return message.message.case === "event" ? message.message.value : undefined;
}
