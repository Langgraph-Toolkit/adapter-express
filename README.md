# @langgraph-toolkit/adapter-express

Express HTTP adapter for a compiled Langgraph-Toolkit registry. It exposes graph listing, typed JSON run requests, and Server-Sent Events without putting Express imports in core.

## Install

```bash
npm install express @langgraph-toolkit/core @langgraph-toolkit/adapter-express
```

## Minimal host

Compose the graph and its runtime once, then mount the adapter. Requests do not repeat actor, policy, model, or checkpoint configuration.

```ts
import express from "express";
import { langgraphRouter, sseMiddleware } from "@langgraph-toolkit/adapter-express";
import { runtime } from "./database-chat/resource.js";

const app = express();
app.use(express.json());
app.use(sseMiddleware);
app.use("/agents", langgraphRouter({
  path: "/agents/:name",
  runtime,
}));

app.listen(Number(process.env.PORT ?? 3000));
```

The adapter serves `GET /agents`, `POST /agents/:name/run`, and `GET /agents/:name/stream`. Add `apiKey` only when the host requires a transport-level key. Actor and policy decisions should still be resolved by the graph runtime.

## Public API

`langgraphRouter`, `sseMiddleware`, `encodeStepEvent`, `LangGraphExpressOptions`, `SseContext`, and `GraphRuntimeError` are the public entrypoints. Keep framework-specific code in the host project.

## Development

```bash
npm install
npm run build
npm test
```

Use the examples project for a complete CLI-scaffolded server with `.env.example`, database-chat composition, and contributor E2E tests.

## License

MIT
