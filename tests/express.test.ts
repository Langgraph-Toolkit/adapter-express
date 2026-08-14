import { describe, it, expect } from "vitest";
import http from "node:http";
import {
  defineGraph,
  node,
  edge,
  safety,
  messagesValue,
} from "@langgraph-toolkit/core";
import { GraphRegistry } from "@langgraph-toolkit/core/runtime";
import { langgraphRouter, sseMiddleware } from "../src/index.js";

interface State {
  messages: unknown[];
  done: boolean;
}

function makeRegistry() {
  const registry = new GraphRegistry();
  const def = defineGraph<State>({
    name: "ping",
    state: {
      messages: messagesValue(),
      done: false as never,
    } as never,
    stateDefaults: { done: false as never },
    nodes: {
      start: node(async () => ({ done: true })),
    },
    entry: "start",
    edges: [edge("start", "END")],
    safety: safety(10),
    interruptBefore: [],
  });
  // GraphRegistry.register compiles and attaches the executor itself.
  registry.register(def);
  return registry;
}

function buildApp(registry: ReturnType<typeof makeRegistry>, apiKey?: string) {
  const express = require("express");
  const app = express();
  app.use(express.json());
  app.use(sseMiddleware);
  // Router pattern is relative to the mount point; mount at root with the
  // name pattern included so routes resolve to /agents/:name/run, /stream.
  app.use(langgraphRouter({ graphs: registry, path: "/agents/:name", apiKey }));
  return app;
}

function request(app: any, method: string, url: string, body?: unknown) {
  return new Promise<{ status: number; body: string }>((resolve, reject) => {
    const server = app.listen(0, "127.0.0.1", () => {
      const addr = server.address() as { port: number };
      const req = http.request(
        { host: "127.0.0.1", port: addr.port, path: url, method, headers: body ? { "content-type": "application/json" } : {} },
        (res) => {
          let data = "";
          res.on("data", (c: string) => (data += c));
          res.on("end", () => {
            server.close();
            resolve({ status: res.statusCode ?? 0, body: data });
          });
        },
      );
      req.on("error", (err) => {
        server.close();
        reject(err);
      });
      if (body) req.write(JSON.stringify(body));
      req.end();
    });
  });
}

describe("adapter-express", () => {
  it("POST /run executes the graph and returns JSON", async () => {
    const app = buildApp(makeRegistry());
    const res = await request(app, "POST", "/agents/ping/run", { messages: [{ role: "user", content: "hi" }] });
    expect(res.status).toBe(200);
    const json = JSON.parse(res.body);
    expect(json.state.done).toBe(true);
    expect(json.stoppedReason).toBe("done");
  });

  it("GET /stream emits SSE step events", async () => {
    const app = buildApp(makeRegistry());
    const res = await request(app, "GET", "/agents/ping/stream?threadId=t1");
    expect(res.status).toBe(200);
    expect(res.body).toContain("event: node_start");
    expect(res.body).toContain("event: node_end");
  });

  it("returns 404 for unregistered graphs", async () => {
    const app = buildApp(makeRegistry());
    const res = await request(app, "POST", "/agents/missing/run", {});
    expect(res.status).toBe(404);
  });

  it("enforces apiKey when configured", async () => {
    const app = buildApp(makeRegistry(), "secret");
    const res = await request(app, "POST", "/agents/ping/run", {});
    expect(res.status).toBe(401);
  });
});
