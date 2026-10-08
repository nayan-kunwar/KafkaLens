# 01 — Two error layers: why "OK" appears for a failed tool call

## The question

In MCP Inspector, `get_topic_metadata` was called with a topic that does not exist
(`topic: "s"`). The **Messages panel shows a green `OK` badge**, yet the call clearly
failed — the middle panel shows a red **Tool Error**:

```json
{
  "code": "NOT_FOUND",
  "message": "Topic \"s\" not found",
  "context": {
    "topic": "s",
    "retryable": true,
    "kafkaType": "UNKNOWN_TOPIC_OR_PARTITION"
  }
}
```

So why does the Inspector say OK?

## TL;DR

There are **two independent layers**, and each has its own success/failure signal:

| Layer                 | What it measures                                        | Failure signal           | Inspector UI           |
| --------------------- | ------------------------------------------------------- | ------------------------ | ---------------------- |
| JSON-RPC exchange     | Did the server return a well-formed response with `id`? | top-level `error` member | red badge / "error"    |
| Tool execution result | Did the tool's logic succeed?                           | `result.isError: true`   | red "Tool Error" panel |

The green **OK badge = layer 1 only**. A tool that returns a structured failure is still
a _perfectly successful_ JSON-RPC response — `id` matches, `result` is present and
schema-valid. The authoritative failure signal for the tool itself is
`result.isError: true` (visible at line 11 of the raw Response in the screenshot).

## What the spec says

From the MCP spec's [Error Handling section](https://modelcontextprotocol.io/specification/2025-06-18/server/tools#error-handling),
tools use **two error reporting mechanisms**:

> 1. **Protocol Errors**: Standard JSON-RPC errors for issues like: Unknown tools,
>    Invalid arguments, Server errors
> 2. **Tool Execution Errors**: Reported in tool results with `isError: true`: API
>    failures, Invalid input data, Business logic errors

Protocol error (layer 1) — the request never produced a tool result:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "error": { "code": -32602, "message": "Unknown tool: invalid_tool_name" }
}
```

Tool execution error (layer 2) — a normal `result`, carrying the failure inside:

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": {
    "content": [{ "type": "text", "text": "Failed to fetch weather data: ..." }],
    "isError": true
  }
}
```

## Real examples from this project

Both layers were observed live against kafka-lens:

| Scenario                                  | Layer | What came back                                                    |
| ----------------------------------------- | ----- | ----------------------------------------------------------------- |
| `get_topic_metadata { topic: "s" }`       | 2     | `result.isError: true` + structured `NOT_FOUND` JSON in `content` |
| Schema-invalid arguments (M2 smoke tests) | 1     | JSON-RPC `error` with `-32602` (validation by the MCP SDK)        |

## Why design it this way?

Tool failures are returned **in-band as readable content** so the AI agent can _reason
about_ the error: read `code: "NOT_FOUND"`, notice the topic name, call `list_topics`,
and retry with a correct name. A bare JSON-RPC error gives the model far less to work
with. Protocol-level errors are reserved for cases where the request can't even become
a meaningful tool execution (unknown tool name, malformed/invalid arguments, server
faults).

## How kafka-lens implements it

`src/tools/run-tool.ts:19-27` — every tool runs through one shared runner:

```ts
(err: unknown) => {
  const appError = err instanceof AppError ? err : new AppError('INTERNAL', 'Tool failed');
  logger.warn({ tool, durationMs, error: appError.toJSON() }, 'tool failed');
  return {
    isError: true,
    content: [{ type: 'text' as const, text: JSON.stringify(appError.toJSON()) }],
  };
};
```

- **Success** → `content` only (`isError` omitted ⇒ false).
- **AppError / unexpected failure** → `isError: true` + the structured error JSON as
  content (code, message, context — never secrets).
- **Invalid arguments** never reach this runner: the MCP SDK validates the Zod input
  schema first and answers with a JSON-RPC `-32602` error (layer 1).

## How to detect it as a client

```js
if (response.error) {
  // layer 1: protocol failure — the tool never ran
} else if (response.result?.isError) {
  // layer 2: tool ran, failed — read response.result.content for details
} else {
  // success — parse response.result.content
}
```

## Takeaways for MCP server authors

1. `isError: true` is a **result**, not a transport failure — clients and dashboards
   that only look at JSON-RPC status will miss it.
2. Return structured, evidence-bearing error payloads as content (`code`, `context`) —
   that's what the LLM reasons over.
3. Reserve JSON-RPC errors for: unknown tool, invalid arguments, protocol violations.
4. When reading a UI: check _which_ layer a status badge refers to before trusting it.
