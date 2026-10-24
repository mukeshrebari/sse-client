# sse-client

A command-line tool that connects to any Server-Sent Events (SSE) endpoint and keeps streaming. If the connection drops, it reconnects on its own and resumes from the last event it saw.

- No dependencies. Needs Node.js 18 or later.
- Supports `GET` and `POST`, any number of headers, and a request body (inline or from a file).
- Events go to stdout. Connection status goes to stderr. So you can pipe the output into `jq`, `grep` or a file.

## Install

```bash
cd tools/sse-client
npm link          # puts an `sse` command on your PATH
# or run it directly without installing:
node tools/sse-client/sse.mjs <url>
```

## Usage

```bash
# Plain GET stream
sse https://example.com/stream

# GET with auth headers, pretty-print JSON data
sse https://api.example.com/events \
  -H "Authorization: Bearer $TOKEN" \
  -H "Ocp-Apim-Subscription-Key: $KEY" \
  --pretty

# POST with a JSON body (for example an LLM streaming endpoint)
sse https://api.example.com/chat -X POST --json-body -d '{"prompt":"hi"}' --no-reconnect

# POST body from a file
sse https://api.example.com/chat -d @body.json --json-body

# Only "update" events, as NDJSON, piped into jq
sse http://localhost:5000/sse -e update --json | jq .data

# Stop after 10 events; reconnect if the server goes silent for 60 s
sse https://example.com/stream -n 10 --idle-timeout 60
```

Run `sse --help` to see every option.

## How reconnect works

| Situation | What the tool does |
|---|---|
| Stream ends, or the network drops | Waits, then reconnects and sends `Last-Event-ID` |
| Server sends `retry: <ms>` | Uses that as the base wait time |
| Connects fail again and again | Doubles the wait each time (3 s, 6 s, 12 s …), capped by `--max-retry` |
| HTTP 5xx, 408 or 429 | Retries |
| Other HTTP 4xx (for example 401, 404) | Stops with exit code 1, because retrying will not help |
| HTTP 204 | Stops with exit code 0, because the SSE spec treats 204 as "stop" |
| `--max-attempts n` reached | Stops with exit code 1 |
| Ctrl+C | Stops cleanly and prints the event count; press it twice to force quit |

`GET` and `POST` behave the same on reconnect: the same method, headers and body are sent again.

## Tests

```bash
npm test
```

The tests start a local SSE server. They cover the parser (CRLF line endings, chunk splits, multi-line data, `retry:`), GET with headers, POST with a body, `Last-Event-ID` resume, 4xx/204/5xx handling and the idle timeout.
