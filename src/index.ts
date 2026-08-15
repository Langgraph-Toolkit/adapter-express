/**
 * @langgraph-toolkit/adapter-express
 *
 * Thin Express binding: an SSE middleware plus a router that exposes
 * registered graphs at {path}/stream (SSE) and {path}/run (JSON), plus the
 * canonical single-resource lifecycle returned by createExpressAdapter().
 */
import { randomUUID } from "node:crypto";
import type { NextFunction, Request, Response, Router } from "express";
import express from "express";
import type { CompiledGraph, GraphDefinition, JsonObject, JsonValue, StepEvent } from "@langgraph-toolkit/core";
import { GraphRegistry } from "@langgraph-toolkit/core/runtime";
import { createGraphLifecycle, ToolkitRuntime, type GraphLifecycle } from "@langgraph-toolkit/core/runtime";
import { GraphRuntimeError } from "@langgraph-toolkit/core";

/** Options for langgraphRouter(); apiKey optionally guards both endpoints. */
export interface LangGraphExpressOptions {
  /** Runtime facade holding the graphs this router exposes. */
  readonly runtime?: ToolkitRuntime;
  /** Backward-compatible registry option. */
  readonly graphs?: GraphRegistry;
  /** Route pattern, e.g. "/agents/:name/run". */
  readonly path: string;
  /** Require an API key header? Pass a validator or string key. */
  readonly apiKey?: string | ((key: string) => boolean);
}

/** Shared SSE write handle for host frameworks that stream StepEvents. */
export interface SseContext {
  readonly writeEvent: (type: string, data: JsonValue) => void;
}

/** Zero-config options for createExpressAdapter(). */
export interface ExpressAdapterOptions extends Omit<LangGraphExpressOptions, "graphs" | "runtime" | "path"> {
  readonly path?: string;
}

/** Express resource returned by createExpressAdapter(). */
export interface ExpressAdapter<TGraph extends object = object> {
  readonly graph: TGraph;
  readonly runtime: GraphRegistry;
  /** Canonical graph lifecycle backing the mounted HTTP routes. */
  readonly lifecycle: GraphLifecycle;
  readonly router: Router;
  readonly middleware: typeof sseMiddleware;
}

/** Set SSE headers before registering the graph router. */
export function sseMiddleware(req: Request, res: Response, next: NextFunction): void {
  if (req.path.endsWith("/stream")) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
  }
  next();
}

function encodeSse(type: string, data: object | JsonValue): string {
  return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

function validateApiKey(req: Request, apiKey?: LangGraphExpressOptions["apiKey"]): boolean {
  if (!apiKey) return true;
  const header = req.headers["x-api-key"];
  const key = Array.isArray(header) ? header[0] : (header ?? "");
  if (typeof apiKey === "string") return key === apiKey;
  return apiKey(String(key ?? ""));
}

function resolveGraphs(options: LangGraphExpressOptions): GraphRegistry {
  const graphs = options.runtime ?? options.graphs;
  if (graphs === undefined) throw new GraphRuntimeError("langgraphRouter requires runtime or graphs.");
  return graphs;
}

/** Register JSON run and SSE stream endpoints for named graphs. */
export function langgraphRouter(options: LangGraphExpressOptions): Router {
  const router: Router = express.Router();
  const base = options.path.replace(/\/(run|stream)$/, "");
  const graphs = resolveGraphs(options);
  const collectionPath = base.endsWith("/:name") ? base.slice(0, -"/:name".length) : base;

  router.get(collectionPath, (_req: Request, res: Response) => {
    res.json(graphs.list());
  });

  router.post(`${base}/run`, async (req: Request, res: Response) => {
    if (!validateApiKey(req, options.apiKey)) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    const name = typeof req.params.name === "string" ? req.params.name : "";
    if (!graphs.has(name)) {
      res.status(404).json({ error: `Graph "${name}" not registered` });
      return;
    }
    try {
      const controller = new AbortController();
      req.on("aborted", () => controller.abort());
      const body = (req.body ?? {}) as JsonObject;
      const result = await graphs.run(name, body, {
        threadId: typeof body.threadId === "string" ? body.threadId : undefined,
        signal: controller.signal,
      });
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : "Graph execution failed" });
    }
  });

  router.get(`${base}/stream`, async (req: Request, res: Response) => {
    if (!validateApiKey(req, options.apiKey)) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    const name = typeof req.params.name === "string" ? req.params.name : "";
    if (!graphs.has(name)) {
      res.status(404).json({ error: `Graph "${name}" not registered` });
      return;
    }
    try {
      const input = typeof req.query.input === "string" ? parseJsonObject(req.query.input) : {};
      const events = graphs.stream(name, input, {
        threadId: typeof req.query.threadId === "string" ? req.query.threadId : undefined,
        signal: abortSignalFromResponse(res),
      });
      for await (const event of events) {
        res.write(encodeSse(event.type, event));
        if (event.type === "error" || event.type === "cancelled") break;
      }
      res.end();
    } catch (err) {
      res.write(encodeSse("error", { message: err instanceof Error ? err.message : "Graph stream failed" }));
      res.end();
    }
  });

  return router;
}

function abortSignalFromResponse(res: Response): AbortSignal {
  const controller = new AbortController();
  res.on("close", () => controller.abort());
  res.on("error", () => controller.abort());
  return controller.signal;
}

function parseJsonObject(value: string): JsonObject {
  try {
    const parsed = JSON.parse(value) as JsonValue;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as JsonObject : {};
  } catch {
    return {};
  }
}

export function encodeStepEvent(event: StepEvent): string {
  return encodeSse(event.type, event);
}

/**
 * Create an Express-ready graph resource from one compiled graph, builder,
 * registry, or runtime. Existing langgraphRouter() remains available when a
 * host needs complete route-level control.
 */
export function createExpressAdapter<TGraph extends object>(graph: TGraph, options: ExpressAdapterOptions = {}): ExpressAdapter<TGraph> {
  const runtime = normalizeGraph(graph);
  const name = resolveGraphName(graph, runtime);
  const lifecycle = createGraphLifecycle(runtime);
  return {
    graph,
    runtime,
    lifecycle,
    router: lifecycleRouter(lifecycle, name, options.apiKey),
    middleware: sseMiddleware,
  };
}

/**
 * Build canonical resource routes relative to an application mount point.
 *
 * `POST /invoke`, `POST /stream`, `POST /resume`, `POST /cancel`, `GET /state`,
 * `GET /history`, `POST /replay`, and `POST /fork` are deliberately named the
 * same across the Express, Fastify, NestJS, and StruxJS adapters.
 */
export function lifecycleRouter(lifecycle: GraphLifecycle, name: string, apiKey?: LangGraphExpressOptions["apiKey"]): Router {
  const router = express.Router();
  const authorize = (req: Request, res: Response): boolean => {
    if (validateApiKey(req, apiKey)) return true;
    res.status(401).json({ error: "Unauthorized" });
    return false;
  };
  const fail = (res: Response, error: unknown): void => {
    res.status(error instanceof GraphRuntimeError ? 409 : 500).json({ error: error instanceof Error ? error.message : "Graph lifecycle failed" });
  };

  router.post("/invoke", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      const request = lifecycleRequest(req.body);
      res.json(await lifecycle.invoke(name, request));
    } catch (error) {
      fail(res, error);
    }
  });
  router.post("/stream", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    const request = lifecycleRequest(req.body);
    const threadId = request.threadId ?? randomUUID();
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();
    const cancelOnClose = (): void => { lifecycle.cancel(name, threadId); };
    req.once("aborted", cancelOnClose);
    res.once("close", cancelOnClose);
    try {
      for await (const event of lifecycle.stream(name, { ...request, threadId })) {
        res.write(encodeSse(event.type, event));
      }
      res.end();
    } catch (error) {
      res.write(encodeSse("error", { message: error instanceof Error ? error.message : "Graph stream failed" }));
      res.end();
    }
  });
  router.post("/resume", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      const body = bodyObject(req.body);
      const threadId = requiredString(body, "threadId");
      const response = "response" in body ? body.response : body.answer;
      if (response === undefined) throw new GraphRuntimeError("resume requires a JSON response or answer field.");
      res.json(await lifecycle.resume(name, { threadId, response }));
    } catch (error) {
      fail(res, error);
    }
  });
  router.post("/cancel", (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      const threadId = requiredString(bodyObject(req.body), "threadId");
      res.json({ cancelled: lifecycle.cancel(name, threadId), threadId });
    } catch (error) {
      fail(res, error);
    }
  });
  router.get("/state", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      res.json(await lifecycle.state(name, queryString(req, "threadId")));
    } catch (error) {
      fail(res, error);
    }
  });
  router.get("/history", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      res.json(await lifecycle.history(name, queryString(req, "threadId")));
    } catch (error) {
      fail(res, error);
    }
  });
  router.post("/replay", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      const body = bodyObject(req.body);
      res.json(await lifecycle.replay(name, {
        ...lifecycleRequest(body),
        threadId: requiredString(body, "threadId"),
        checkpointId: requiredString(body, "checkpointId"),
      }));
    } catch (error) {
      fail(res, error);
    }
  });
  router.post("/fork", async (req: Request, res: Response) => {
    if (!authorize(req, res)) return;
    try {
      const body = bodyObject(req.body);
      res.json(await lifecycle.fork(name, {
        threadId: requiredString(body, "threadId"),
        checkpointId: requiredString(body, "checkpointId"),
        targetThreadId: requiredString(body, "targetThreadId"),
      }));
    } catch (error) {
      fail(res, error);
    }
  });
  return router;
}

function normalizeGraph<TGraph extends object>(graph: TGraph): GraphRegistry {
  if (graph instanceof ToolkitRuntime) return graph;
  const runtime = new GraphRegistry();
  const source = graph as object;
  const collection = source as { readonly list?: () => string[]; readonly get?: (name: string) => CompiledGraph<object> | undefined };
  if (typeof collection.list === "function" && typeof collection.get === "function") {
    for (const name of collection.list()) {
      const compiled = collection.get(name);
      if (compiled && !runtime.has(compiled.name)) runtime.add(compiled);
    }
    return runtime;
  }
  const executable = source as { readonly name?: string; readonly definition?: GraphDefinition<object>; readonly run?: (input: object) => Promise<object>; readonly stream?: (input: object) => AsyncIterable<object> };
  if (typeof executable.name === "string" && executable.definition !== undefined && typeof executable.run === "function" && typeof executable.stream === "function") {
    runtime.add(graph as CompiledGraph<object>);
    return runtime;
  }
  const builder = source as { readonly build?: () => CompiledGraph<object> };
  if (typeof builder.build === "function") {
    runtime.add(builder.build());
    return runtime;
  }
  throw new GraphRuntimeError("createExpressAdapter requires a compiled graph, graph builder, runtime, or registry.");
}

function resolveGraphName<TGraph extends object>(graph: TGraph, runtime: GraphRegistry): string {
  const named = graph as { readonly name?: string };
  if (typeof named.name === "string" && runtime.has(named.name)) return named.name;
  const names = runtime.list();
  if (names.length === 1) return names[0] as string;
  throw new GraphRuntimeError("createExpressAdapter requires one compiled graph. Use langgraphRouter() when mounting a graph collection.");
}

function bodyObject(value: JsonValue): JsonObject {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) return value as JsonObject;
  return {};
}

function lifecycleRequest(value: JsonValue): { readonly input: JsonObject; readonly threadId?: string } {
  const body = bodyObject(value);
  const input = "input" in body ? bodyObject(body.input) : withoutLifecycleFields(body);
  return typeof body.threadId === "string" ? { input, threadId: body.threadId } : { input };
}

function withoutLifecycleFields(body: JsonObject): JsonObject {
  const result: Record<string, JsonValue> = {};
  for (const [key, value] of Object.entries(body)) {
    if (!["threadId", "checkpointId", "targetThreadId", "response", "answer"].includes(key)) result[key] = value;
  }
  return result;
}

function requiredString(body: JsonObject, key: string): string {
  const value = body[key];
  if (typeof value !== "string" || value.length === 0) throw new GraphRuntimeError(`${key} is required.`);
  return value;
}

function queryString(req: Request, key: string): string {
  const value = req.query[key];
  if (typeof value !== "string" || value.length === 0) throw new GraphRuntimeError(`${key} query parameter is required.`);
  return value;
}

export { GraphRuntimeError };
