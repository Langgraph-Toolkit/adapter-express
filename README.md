# @langgraph-toolkit/adapter-express

**Keep the graph portable and let Express absorb the HTTP wiring.** The adapter exposes a compiled graph, builder, registry, or runtime through JSON and Server-Sent Events without moving checkpoint, actor, policy, provider, or MCP configuration into request handlers.

## Install

```bash
npm install express @langgraph-toolkit/core @langgraph-toolkit/adapter-express
```

## Zero-config factory

```ts
import express from "express";
import { createExpressAdapter } from "@langgraph-toolkit/adapter-express";
import { resource } from "./resource.js";

const adapter = createExpressAdapter(resource.runtime);
const app = express();

app.use(express.json());
app.use(adapter.middleware);
app.use("/agents", adapter.router);
app.listen(Number(process.env.PORT ?? 3000));
```

The factory normalizes one graph or an existing registry and returns `{ graph, runtime, router, middleware }`. It defaults to `/agents/:name/run` and `/agents/:name/stream`; pass `path` or `apiKey` only when needed.

## Host-native escape hatch

When a host needs complete route-level control, use `langgraphRouter({ runtime, path, apiKey })` and `sseMiddleware` directly. The adapter serves graph listing, JSON execution, and typed SSE events while the resource remains framework-neutral.

| Concern | Express adapter | Core or resource |
|---|---|---|
| JSON parsing and response lifecycle | Yes | No |
| SSE headers and event encoding | Yes | No |
| Graph topology and typed state | No | Yes |
| MCP credentials and tool policy | No | MCP/resource |
| Checkpoint and actor defaults | No | Graph runtime |

## Development

```bash
npm install
npm run build
npm test
```

## License

MIT
