// Unit tests for channel multiplexing / demux in SealantClient. No daemon: a controllable in-memory
// Duplex stands in for the connection, so we can inject synthetic `ServerMessage::Stream` frames and
// assert they route to the right `Channel` by channel_id, and that a `StreamEnd` closes the channel.

import { test } from "node:test";
import assert from "node:assert/strict";
import { Duplex } from "node:stream";
import { Buffer } from "node:buffer";

import { fromBinary } from "@bufbuild/protobuf";

import { SealantClient } from "@sealant/runtime-client";
import {
  create,
  encodeServer,
  encodeFrame,
  ServerMessageSchema,
  ClientMessageSchema,
  type StreamFrame,
} from "@sealant/runtime-protocol";

/**
 * A Duplex the test fully controls: `inject()` pushes daemon→client bytes (the client reads these via
 * its "data" handler); everything the client writes is captured in `written` for assertions.
 */
class MockConn extends Duplex {
  readonly written: Buffer[] = [];
  _read(): void {}
  _write(chunk: Buffer, _enc: BufferEncoding, cb: (e?: Error | null) => void): void {
    this.written.push(Buffer.from(chunk));
    cb();
  }
  /** Push a fully-framed ServerMessage to the client. */
  inject(message: Parameters<typeof encodeServer>[0]): void {
    this.push(encodeFrame(encodeServer(message)));
  }
}

/** Build a `ServerMessage::Stream` for `channelId` carrying a data payload. */
function dataFrame(channelId: string, bytes: number[]) {
  return create(ServerMessageSchema, {
    message: {
      case: "stream",
      value: { channelId, seq: 0n, payload: { case: "data", value: new Uint8Array(bytes) } },
    },
  });
}

/** Build a `ServerMessage::Stream` for `channelId` carrying an End payload. */
function endFrame(channelId: string, exitCode?: number) {
  return create(ServerMessageSchema, {
    message: {
      case: "stream",
      value: { channelId, seq: 1n, payload: { case: "end", value: { exitCode } } },
    },
  });
}

/** Build a `ServerMessage::Stream` for `channelId` carrying a WindowUpdate payload. */
function windowFrame(channelId: string, credits: bigint) {
  return create(ServerMessageSchema, {
    message: {
      case: "stream",
      value: { channelId, seq: 2n, payload: { case: "windowUpdate", value: { credits } } },
    },
  });
}

/** Read at most `n` chunks from a channel's async iterator (stops early if it closes). */
async function take(channel: AsyncIterable<Uint8Array>, n: number): Promise<Uint8Array[]> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of channel) {
    chunks.push(chunk);
    if (chunks.length >= n) break;
  }
  return chunks;
}

test("demux routes frames for channel A vs B to the correct channel", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);

  const a = client.openChannel("chan-A");
  const b = client.openChannel("chan-B");

  conn.inject(dataFrame("chan-A", [0x61, 0x61])); // "aa"
  conn.inject(dataFrame("chan-B", [0x62])); // "b"
  conn.inject(dataFrame("chan-A", [0x61])); // "a"
  conn.inject(dataFrame("chan-B", [0x62, 0x62])); // "bb"

  const aChunks = await take(a, 2);
  const bChunks = await take(b, 2);

  assert.deepEqual(aChunks.map((c) => [...c]), [[0x61, 0x61], [0x61]]);
  assert.deepEqual(bChunks.map((c) => [...c]), [[0x62], [0x62, 0x62]]);

  client.close();
});

test("a StreamEnd frame closes only its own channel", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);

  const a = client.openChannel("chan-A");
  const b = client.openChannel("chan-B");

  conn.inject(dataFrame("chan-A", [0x01]));
  conn.inject(endFrame("chan-A", 0));
  conn.inject(dataFrame("chan-B", [0x02]));

  // Draining A yields the one data chunk then completes because of the End.
  const aChunks: Uint8Array[] = [];
  for await (const chunk of a) aChunks.push(chunk);
  assert.deepEqual(aChunks.map((c) => [...c]), [[0x01]]);
  assert.equal(a.isClosed, true);

  const cause = await a.closed;
  assert.equal(cause.kind, "remote");
  if (cause.kind === "remote") assert.equal(cause.end.exitCode, 0);

  // B is untouched and still delivers its data.
  assert.equal(b.isClosed, false);
  const bChunks = await take(b, 1);
  assert.deepEqual(bChunks.map((c) => [...c]), [[0x02]]);

  client.close();
});

test("frames for an unknown/closed channel are dropped (no throw)", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);
  const a = client.openChannel("chan-A");

  conn.inject(endFrame("chan-A"));
  await a.closed;

  // Late frames for the now-released channel must not crash the connection.
  conn.inject(dataFrame("chan-A", [0x99]));
  conn.inject(dataFrame("never-opened", [0x99]));

  // The client is still usable: open a fresh channel and route to it.
  const c = client.openChannel("chan-C");
  conn.inject(dataFrame("chan-C", [0x43]));
  const chunks = await take(c, 1);
  assert.deepEqual(chunks.map((x) => [...x]), [[0x43]]);

  client.close();
});

test("channel.write muxes an outbound ClientMessage::Stream data frame", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);
  const a = client.openChannel("chan-A");

  a.write(new Uint8Array([0x68, 0x69])); // "hi"

  // Decode what the client wrote back over the wire.
  const buf = Buffer.concat(conn.written);
  const len = buf.readUInt32BE(0);
  const msg = fromBinary(ClientMessageSchema, buf.subarray(4, 4 + len));
  assert.equal(msg.message.case, "stream");
  const frame = (msg.message as { value: StreamFrame }).value;
  assert.equal(frame.channelId, "chan-A");
  assert.equal(frame.payload.case, "data");
  if (frame.payload.case === "data") assert.deepEqual([...frame.payload.value], [0x68, 0x69]);

  client.close();
});

test("an inbound WindowUpdate releases an awaitWindow waiter", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);
  const a = client.openChannel("chan-A");

  const credits = a.awaitWindow();
  conn.inject(windowFrame("chan-A", 4096n));
  assert.equal(await credits, 4096n);

  client.close();
});

test("connection close fails open channels", async () => {
  const conn = new MockConn();
  const client = SealantClient.fromStream(conn);
  const a = client.openChannel("chan-A");

  client.close();
  conn.destroy();

  const cause = await a.closed;
  assert.equal(cause.kind, "error");
  assert.equal(a.isClosed, true);
});
