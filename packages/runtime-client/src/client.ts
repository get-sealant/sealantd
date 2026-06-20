// @sealant/runtime-client
//
// Ergonomic TypeScript client for sealantd over the Protobuf wire (ADR-0012). Uses IPC (a Unix
// domain socket) as the language boundary — never in-process FFI (plan §19).

import net from "node:net";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { Buffer } from "node:buffer";
import { setTimeout as delay } from "node:timers/promises";

import {
  encodeClient,
  encodeFrame,
  FrameDecoder,
  SCHEMA_VERSION,
} from "../../runtime-protocol/src/index.ts";
import type {
  Command,
  ServerMessage,
} from "../../runtime-protocol/src/index.ts";

/** Error raised when the daemon returns a typed control error. */
export class SealantError extends Error {
  readonly code: string;
  readonly detail: unknown;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(error: any) {
    super(error?.message ?? "control error");
    this.name = "SealantError";
    this.code = error?.code ?? "CONTROL_ERROR_CODE_UNSPECIFIED";
    this.detail = error?.detailJson;
  }
}

type Pending = {
  resolve: (response: ServerMessage) => void;
  reject: (error: Error) => void;
};

/** The result data of a successful response (the active CommandResult oneof case). */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function unwrap(response: any): any {
  const outcome = response.outcome ?? {};
  if (outcome.error) {
    throw new SealantError(outcome.error);
  }
  return outcome.ok ?? {};
}

export interface ExecArgs {
  executable: string;
  args?: string[];
  executionId?: string;
  sessionId?: string;
  cwd?: string;
  stdin?: boolean;
  timeoutMillis?: number;
  background?: boolean;
}

/** A connected control client for one sealantd instance. */
export class SealantClient {
  #socket: net.Socket;
  #decoder: FrameDecoder = new FrameDecoder();
  #pending: Map<string, Pending> = new Map();
  #counter = 0;
  #closed = false;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #eventQueue: any[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #eventWaiters: Array<(result: IteratorResult<any>) => void> = [];

  constructor(socket: net.Socket) {
    this.#socket = socket;
    this.#socket.on("data", (chunk: Buffer) => this.#onData(chunk));
    this.#socket.on("close", () => this.#onClose());
    this.#socket.on("error", () => {});
  }

  static async connect(
    socketPath: string,
    options: { retries?: number; delayMs?: number } = {},
  ): Promise<SealantClient> {
    const retries = options.retries ?? 100;
    const delayMs = options.delayMs ?? 20;
    let lastError: unknown;
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        return new SealantClient(await connectOnce(socketPath));
      } catch (error) {
        lastError = error;
        await delay(delayMs);
      }
    }
    throw lastError instanceof Error ? lastError : new Error("connection failed");
  }

  static async spawn(options: {
    binPath: string;
    socketPath: string;
    workspace?: string;
    sandboxId?: string;
    executionId?: string;
    spoolDir?: string;
    logLevel?: string;
  }): Promise<{ client: SealantClient; child: ChildProcess }> {
    const args = ["--socket", options.socketPath, "--log-level", options.logLevel ?? "off"];
    if (options.workspace) args.push("--workspace", options.workspace);
    if (options.sandboxId) args.push("--sandbox-id", options.sandboxId);
    if (options.executionId) args.push("--execution-id", options.executionId);
    if (options.spoolDir) args.push("--spool-dir", options.spoolDir);
    const child = spawn(options.binPath, args, { stdio: ["ignore", "ignore", "inherit"] });
    try {
      return { client: await SealantClient.connect(options.socketPath), child };
    } catch (error) {
      child.kill("SIGKILL");
      throw error;
    }
  }

  /** Send a command (a protobuf oneof object) and await its single response. */
  request(command: Command): Promise<ServerMessage> {
    if (this.#closed) {
      return Promise.reject(new Error("client is closed"));
    }
    const requestId = `req_client_${++this.#counter}`;
    const body = encodeClient({ schemaVersion: SCHEMA_VERSION, requestId, command });
    return new Promise((resolve, reject) => {
      this.#pending.set(requestId, { resolve, reject });
      this.#socket.write(encodeFrame(body), (error) => {
        if (error) {
          this.#pending.delete(requestId);
          reject(error);
        }
      });
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async health(): Promise<any> {
    return unwrap((await this.request({ runtimeHealth: {} })).response).health;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async getCapabilities(): Promise<any> {
    return unwrap((await this.request({ runtimeGetCapabilities: {} })).response).capabilities;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async exec(args: ExecArgs): Promise<any> {
    return unwrap((await this.request({ exec: args })).response).execAccepted;
  }

  async writeStdin(processId: string, data: Buffer): Promise<void> {
    unwrap((await this.request({ writeStdin: { processId, data } })).response);
  }

  async shutdown(graceMillis?: number): Promise<void> {
    const args = graceMillis === undefined ? {} : { graceMillis };
    unwrap((await this.request({ runtimeGracefulShutdown: args })).response);
  }

  /** Async iterator over telemetry events (the protobuf EventEnvelope shape). */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  events(): AsyncIterableIterator<any> {
    const self = this;
    return {
      [Symbol.asyncIterator]() {
        return this;
      },
      next(): Promise<IteratorResult<unknown>> {
        const queued = self.#eventQueue.shift();
        if (queued !== undefined) {
          return Promise.resolve({ value: queued, done: false });
        }
        if (self.#closed) {
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise((resolve) => self.#eventWaiters.push(resolve));
      },
      return(): Promise<IteratorResult<unknown>> {
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  close(): void {
    this.#socket.end();
  }

  #onData(chunk: Buffer): void {
    let messages: ServerMessage[];
    try {
      messages = this.#decoder.push(chunk);
    } catch (error) {
      this.#socket.destroy(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    for (const message of messages) {
      if (message.response) {
        const pending = this.#pending.get(message.response.requestId);
        if (pending) {
          this.#pending.delete(message.response.requestId);
          pending.resolve(message);
        }
      } else if (message.event) {
        this.#emitEvent(message.event);
      }
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #emitEvent(event: any): void {
    const waiter = this.#eventWaiters.shift();
    if (waiter) {
      waiter({ value: event, done: false });
    } else {
      this.#eventQueue.push(event);
    }
  }

  #onClose(): void {
    this.#closed = true;
    for (const waiter of this.#eventWaiters.splice(0)) {
      waiter({ value: undefined, done: true });
    }
    for (const pending of this.#pending.values()) {
      pending.reject(new Error("connection closed"));
    }
    this.#pending.clear();
  }
}

function connectOnce(socketPath: string): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    const onError = (error: Error) => {
      socket.destroy();
      reject(error);
    };
    socket.once("error", onError);
    socket.once("connect", () => {
      socket.removeListener("error", onError);
      resolve(socket);
    });
  });
}
