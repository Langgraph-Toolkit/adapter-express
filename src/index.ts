/**
 * @langgraph-toolkit/adapter-express
 *
 * Thin Express binding: an SSE middleware plus a router that exposes
 * compiled graphs at {path}/stream (SSE) and {path}/run (JSON).
 *
 * Install: npm install express @langgraph-toolkit/adapter-express
 * Peer: express
 */
import type { NextFunction, Request, Response, Router } from "express";
import express from "express";
import type { JsonObject, JsonValue, StepEvent } from "@langgraph-toolkit/core";
import { GraphRuntimeError } from "@langgraph-toolkit/core";
import type { GraphRegistry } from "@langgraph-toolkit/core";

/** Options for langgraphRouter(); apiKey optionally guards both endpoints. */
export interface LangGraphExpressOptions {
  /** Named registry holding the graphs this router exposes. */
  graphs: GraphRegistry;
  /** Route pattern, e.g. "/agents/:name/run". */
  path: string;
  /** Require an API key header? Pass a validator or string key. */
  apiKey?: string | ((key: string) => boolean);
}

/** Shared SSE write handle for host frameworks that stream StepEvents. */
export interface SseContext {
  writeEvent(type: string, data: JsonValue): void;
}

/**
 * SSE middleware: sets headers once per request lifecycle. Mount before the
 * langgraphRouter so /stream responses carry the correct SSE headers.
 */
export function sseMiddleware(req: Request, res: Response, next: NextFunction): void {
  if (req.path.endsWith("/stream")) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();
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

/**
 * Register graph endpoints under the given path pattern.
 *   GET  {path}/stream  -> SSE step events
 *   POST {path}/run     -> JSON run result
 */
export function langgraphRouter(options: LangGraphExpressOptions): Router {
  const router: Router = express.Router();
  const base = options.path.replace(/\/(run|stream)$/, "");

  router.post(`${base}/run`, async (req: Request, res: Response) => {
    if (!validateApiKey(req, options.apiKey)) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    const name = req.params.name;
    const compiled = options.graphs.get(name);
    if (!compiled) {
      res.status(404).json({ error: `Graph "${name}" not registered` });
      return;
    }
    try {
      // Cancellation only when the CLIENT disconnects (Rule L2): bind to the
      // request abort event, NOT the response close event (which fires on
      // every normal send and would falsely cancel a long run).
      const controller = new AbortController();
      req.on("aborted", () => controller.abort());
      const body = (req.body ?? {}) as JsonObject;
      const result = await compiled.run(body, {
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
    const name = req.params.name;
    const compiled = options.graphs.get(name);
    if (!compiled) {
      res.status(404).json({ error: `Graph "${name}" not registered` });
      return;
    }
    try {
      const input = typeof req.query.input === "string" ? parseJsonObject(req.query.input) : {};
      const events = compiled.stream(input, {
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

/**
 * Convert a response close event into an AbortSignal so the graph can cancel
 * when the client drops the SSE connection (Rule L2). Only the /stream
 * handler uses this; /run binds to the request abort event instead.
 */
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

export { GraphRuntimeError };
