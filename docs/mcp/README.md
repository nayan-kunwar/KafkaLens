# MCP learning notes

Personal notes on the **Model Context Protocol**, learned hands-on by building this
repo's MCP server (kafka-lens). Every note is grounded in either the official MCP spec
or something actually observed in this project — no generic tutorial summaries.

## Notes

| #   | Note                                           | What it answers                                                  |
| --- | ---------------------------------------------- | ---------------------------------------------------------------- |
| 01  | [Two error layers](01-error-handling.md)       | Why MCP Inspector shows "OK" even when a tool call failed        |
| 02  | [stdio transport](02-stdio-transport.md)       | Why stdout is reserved exclusively for MCP messages              |
| 03  | [Tools & Inspector](03-tools-and-inspector.md) | How `tools/list` → `tools/call` works and how to explore it live |

## Sources of truth

- [MCP spec — Tools (error handling section)](https://modelcontextprotocol.io/specification/2025-06-18/server/tools)
- [MCP spec — Transports (stdio section)](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports)
- [MCP spec — Lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle)
- Protocol version negotiated in this project's sessions: `2024-11-05`

## Conventions

- Claims link back to a spec section or a file in this repository (`src/...`).
- Real payloads pasted from actual sessions (MCP Inspector + stdio smoke tests).
- New notes go in as numbered files and get registered in the table above.
