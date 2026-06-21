// @sealant/runtime-client — channel multiplexing.
//
// A `Channel` is one logical byte conduit inside the single length-prefixed control connection
// (ADR-0012). The daemon addresses frames by `channelId`; the client demuxes inbound
// `ServerMessage::Stream` frames into the matching `Channel` and the `Channel` muxes outbound bytes
// back out as `ClientMessage::Stream` frames. This is the substrate the gateway builds SSH channels
// (session attach, direct-tcpip forwards, SFTP subsystem) on top of.

import type { StreamEnd, StreamWindowUpdate } from "@sealant/runtime-protocol";

/** How a `Channel` writes a frame back to the daemon over the shared connection. The client wires
 * this to its framed-socket writer; the channel never touches the socket directly. */
export interface ChannelTransport {
  /** Send a `StreamFrame::Data` for this channel. */
  sendData(channelId: string, data: Uint8Array): void;
  /** Send a `StreamFrame::WindowUpdate` (flow-control credits) for this channel. */
  sendWindowUpdate(channelId: string, credits: bigint): void;
  /** Send a `StreamFrame::End` for this channel (half-close from the client side). */
  sendEnd(channelId: string, end?: StreamEnd): void;
  /** Detach this channel from the client's demux table once it is fully closed. */
  release(channelId: string): void;
}

/** Why a channel closed: a daemon `StreamEnd`, a local `end()`/`destroy()`, or the connection dying. */
export type ChannelClose =
  | { kind: "remote"; end: StreamEnd }
  | { kind: "local" }
  | { kind: "error"; error: Error };

/**
 * One demultiplexed byte channel. It is an async-iterable of inbound `Uint8Array` chunks (the bytes
 * the daemon wrote on this `channelId`) and exposes `write`/`windowUpdate`/`end` for outbound bytes.
 *
 * Backpressure-friendly: inbound chunks queue until consumed; `closed` resolves with the close cause.
 */
export class Channel implements AsyncIterable<Uint8Array> {
  /** The daemon-assigned channel id this conduit is bound to. */
  readonly channelId: string;

  #transport: ChannelTransport;
  #inbound: Uint8Array[] = [];
  #waiters: Array<(result: IteratorResult<Uint8Array>) => void> = [];
  #windowWaiters: Array<(credits: bigint) => void> = [];
  #closed = false;
  #close?: ChannelClose;
  #resolveClosed!: (cause: ChannelClose) => void;

  /** Resolves with the cause when the channel is fully closed (remote End, local end, or error). */
  readonly closed: Promise<ChannelClose>;

  constructor(channelId: string, transport: ChannelTransport) {
    this.channelId = channelId;
    this.#transport = transport;
    this.closed = new Promise((resolve) => {
      this.#resolveClosed = resolve;
    });
  }

  /** True once the channel has been closed from either side. */
  get isClosed(): boolean {
    return this.#closed;
  }

  /** The close cause, if the channel is closed; otherwise `undefined`. */
  get closeCause(): ChannelClose | undefined {
    return this.#close;
  }

  // --- inbound (demux target; called by the client) -------------------------------------------

  /** Route an inbound `StreamFrame::Data` payload into this channel's byte stream. */
  pushData(data: Uint8Array): void {
    if (this.#closed) return;
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter({ value: data, done: false });
    } else {
      this.#inbound.push(data);
    }
  }

  /** Route an inbound `StreamFrame::WindowUpdate`: release outbound writers waiting on credits. */
  pushWindowUpdate(update: StreamWindowUpdate): void {
    if (this.#closed) return;
    for (const w of this.#windowWaiters.splice(0)) w(update.credits);
  }

  /** Route an inbound `StreamFrame::End`: drain queued bytes, then close the iterator. */
  pushEnd(end: StreamEnd): void {
    this.#finish({ kind: "remote", end });
  }

  /** The connection died under us; fail the channel. */
  fail(error: Error): void {
    this.#finish({ kind: "error", error });
  }

  // --- outbound (mux source; called by the consumer) ------------------------------------------

  /** Write bytes to the daemon as a `StreamFrame::Data` on this channel. No-op once closed. */
  write(data: Uint8Array): void {
    if (this.#closed) throw new Error(`channel ${this.channelId} is closed`);
    this.#transport.sendData(this.channelId, data);
  }

  /** Grant the daemon `credits` more bytes of send window (`StreamFrame::WindowUpdate`). */
  windowUpdate(credits: bigint): void {
    if (this.#closed) throw new Error(`channel ${this.channelId} is closed`);
    this.#transport.sendWindowUpdate(this.channelId, credits);
  }

  /** Await the next inbound `WindowUpdate`'s credit count (for outbound flow control). */
  awaitWindow(): Promise<bigint> {
    return new Promise((resolve) => this.#windowWaiters.push(resolve));
  }

  /** Half-close: send a `StreamFrame::End` to the daemon and close this channel locally. */
  end(end?: StreamEnd): void {
    if (this.#closed) return;
    this.#transport.sendEnd(this.channelId, end);
    this.#finish({ kind: "local" });
  }

  // --- async iteration --------------------------------------------------------------------------

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return {
      next: (): Promise<IteratorResult<Uint8Array>> => {
        const queued = this.#inbound.shift();
        if (queued !== undefined) return Promise.resolve({ value: queued, done: false });
        if (this.#closed) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => this.#waiters.push(resolve));
      },
      return: (): Promise<IteratorResult<Uint8Array>> => {
        this.end();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  /** Internal: mark closed once, drain waiters, resolve `closed`, and detach from the demux table. */
  #finish(cause: ChannelClose): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#close = cause;
    for (const waiter of this.#waiters.splice(0)) {
      waiter({ value: undefined, done: true });
    }
    for (const w of this.#windowWaiters.splice(0)) w(0n);
    this.#transport.release(this.channelId);
    this.#resolveClosed(cause);
  }
}
