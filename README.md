# @langgraph-toolkit/adapter-express

**Keep the graph portable and let Express absorb the HTTP wiring.** This adapter exposes a compiled Langgraph-Toolkit registry through Express routes and Server-Sent Events. It does not move checkpoint, actor, policy, provider, or MCP configuration into request handlers.

## Install

```bash
npm install express @langgraph-toolkit/core @langgraph-toolkit/adapter-express
```

## Minimal host wiring

Compose the graph resource once, then mount the adapter. The request only carries business input and, when resuming, a thread identifier.

```ts
import express from "express";
import { langgraphRouter, sseMiddleware } from "@langgraph-toolkit/adapter-express";
import { runtime } from "./database-chat/resource.js";

const app = express();
app.use(express.json());
app.use(sseMiddleware);
app.use("/agents", langgraphRouter({ runtime }));

app.listen(Number(process.env.PORT ?? 3000));
```

The adapter serves `GET /agents`, `POST /agents/:name/run`, and `GET /agents/:name/stream`. Add a transport key only when the host requires one. Actor and policy decisions remain graph runtime concerns.

## Why the boundary stays flexible

| Application concern | Express adapter | Core or resource |
|---|---|---|
| JSON parsing and response lifecycle | Yes | No |
| SSE headers and event encoding | Yes | No |
| Graph topology and typed state | No | Yes |
| MCP credentials and tool policy | No | MCP/resource |
| Checkpoint and actor defaults | No | Graph runtime |

The same resource can move to Fastify, NestJS, StruxJS, a worker, or a custom server without rewriting its nodes.

## Public API and development

The public entrypoints are `langgraphRouter`, `sseMiddleware`, `encodeStepEvent`, `LangGraphExpressOptions`, `SseContext`, and `GraphRuntimeError`.

```bash
npm install
npm run build
npm test
```

See `examples/projects/express` for a complete CLI-scaffolded database-chat project.

## License

MIT
