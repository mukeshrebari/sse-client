#!/usr/bin/env node
// sse — stream any Server-Sent Events (SSE) endpoint continuously.
// Zero dependencies. Needs Node.js 18 or later (uses built-in fetch).
//
// Events go to stdout; connection status goes to stderr, so output can be piped.
// Reconnects automatically and resumes with the Last-Event-ID header.

import { readFileSync, realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const HELP = `Usage: sse <url> [options]

Stream a Server-Sent Events endpoint and print each event as it arrives.

Request:
  -X, --method <verb>       GET or POST (default GET, or POST when --data is set)
  -H, --header "K: V"       Add a request header (repeatable)
  -d, --data <body>         Request body; use @file to read it from a file
      --json-body           Shortcut for -H "Content-Type: application/json"
      --last-event-id <id>  Start from this event id
  -k, --insecure            Skip TLS certificate checks

Reconnect:
      --no-reconnect        Exit when the stream ends instead of reconnecting
      --retry <ms>          Base reconnect delay (default 3000; server "retry:" overrides)
      --max-retry <ms>      Cap for the back-off delay (default 30000)
      --max-attempts <n>    Give up after n failed connects in a row (default: never)
      --idle-timeout <s>    Reconnect if nothing arrives for s seconds (default: off)

Output:
  -e, --event <name>        Only show events with this name (repeatable)
      --json                One JSON object per event (NDJSON)
      --pretty              Pretty-print data that is valid JSON
      --data-only           Print only the data field
      --raw                 Print the raw stream lines as received
      --comments            Also show ": comment" / keep-alive lines
  -n, --max-events <n>      Exit after n events
  -q, --quiet               No status messages on stderr
  -h, --help                Show this help

Examples:
  sse https://example.com/stream
  sse https://api.example.com/events -H "Authorization: Bearer $TOKEN" --pretty
  sse https://api.example.com/chat -X POST --json-body -d '{"q":"hi"}' --no-reconnect
  sse https://api.example.com/chat -d @body.json --json-body
  sse http://localhost:5000/sse --json | jq .data
`;

// ---------- SSE parser (WHATWG HTML spec, "event stream interpretation") ----------

export class SseParser {
  constructor({ onEvent, onComment, onRetry, onLine } = {}) {
    this.onEvent = onEvent ?? (() => {});
    this.onComment = onComment ?? (() => {});
    this.onRetry = onRetry ?? (() => {});
    this.onLine = onLine ?? (() => {});
    this.buffer = '';
    this.firstChunk = true;
    this.pendingCR = false;
    this.lastEventId = '';
    this.resetEvent();
  }

  resetEvent() {
    this.eventType = '';
    this.dataLines = [];
    this.hasData = false;
  }

  feed(text) {
    if (this.firstChunk && text.length) {
      this.firstChunk = false;
      if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
    }
    // A "\r\n" split across two chunks must count as one line break.
    if (this.pendingCR && text.startsWith('\n')) text = text.slice(1);
    this.pendingCR = false;

    this.buffer += text;
    const lineBreak = /\r\n|\r|\n/g;
    let start = 0;
    let match;
    while ((match = lineBreak.exec(this.buffer)) !== null) {
      this.processLine(this.buffer.slice(start, match.index));
      start = match.index + match[0].length;
      // A trailing lone "\r" may be the first half of a "\r\n"; skip a leading "\n" next time.
      if (match[0] === '\r' && start === this.buffer.length) this.pendingCR = true;
    }
    this.buffer = this.buffer.slice(start);
  }

  processLine(line) {
    this.onLine(line);
    if (line === '') return this.dispatch();
    if (line.startsWith(':')) return this.onComment(line.slice(1).replace(/^ /, ''));

    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);

    switch (field) {
      case 'event':
        this.eventType = value;
        break;
      case 'data':
        this.dataLines.push(value);
        this.hasData = true;
        break;
      case 'id':
        if (!value.includes('\0')) this.lastEventId = value;
        break;
      case 'retry':
        if (/^\d+$/.test(value)) this.onRetry(Number(value));
        break;
      default:
        break; // Unknown fields are ignored, as the spec requires.
    }
  }

  dispatch() {
    if (this.hasData) {
      this.onEvent({
        id: this.lastEventId,
        event: this.eventType || 'message',
        data: this.dataLines.join('\n'),
      });
    }
    this.resetEvent();
  }
}

// ---------- CLI ----------

function fail(message) {
  process.stderr.write(`sse: ${message}\n`);
  process.exit(2);
}

export function parseCli(argv) {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        header: { type: 'string', short: 'H', multiple: true, default: [] },
        method: { type: 'string', short: 'X' },
        data: { type: 'string', short: 'd' },
        'json-body': { type: 'boolean', default: false },
        'last-event-id': { type: 'string' },
        insecure: { type: 'boolean', short: 'k', default: false },
        'no-reconnect': { type: 'boolean', default: false },
        retry: { type: 'string', default: '3000' },
        'max-retry': { type: 'string', default: '30000' },
        'max-attempts': { type: 'string' },
        'idle-timeout': { type: 'string' },
        event: { type: 'string', short: 'e', multiple: true, default: [] },
        json: { type: 'boolean', default: false },
        pretty: { type: 'boolean', default: false },
        'data-only': { type: 'boolean', default: false },
        raw: { type: 'boolean', default: false },
        comments: { type: 'boolean', default: false },
        'max-events': { type: 'string', short: 'n' },
        quiet: { type: 'boolean', short: 'q', default: false },
        help: { type: 'boolean', short: 'h', default: false },
      },
    });
  } catch (err) {
    fail(`${err.message}\nRun "sse --help" for usage.`);
  }
  const { values, positionals } = parsed;

  if (values.help || positionals.length === 0) {
    process.stdout.write(HELP);
    process.exit(values.help ? 0 : 2);
  }

  const toNumber = (name, raw) => {
    if (raw === undefined) return undefined;
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 0) fail(`--${name} must be a non-negative number, got "${raw}"`);
    return n;
  };

  const headers = {};
  if (values['json-body']) headers['Content-Type'] = 'application/json';
  for (const h of values.header) {
    const i = h.indexOf(':');
    if (i <= 0) fail(`Bad header "${h}". Use "Name: value".`);
    headers[h.slice(0, i).trim()] = h.slice(i + 1).trim();
  }

  let body;
  if (values.data !== undefined) {
    if (values.data.startsWith('@')) {
      try {
        body = readFileSync(values.data.slice(1), 'utf8');
      } catch (err) {
        fail(`Cannot read body file: ${err.message}`);
      }
    } else {
      body = values.data;
    }
  }

  const method = (values.method ?? (body !== undefined ? 'POST' : 'GET')).toUpperCase();
  if (!['GET', 'POST'].includes(method)) fail(`--method must be GET or POST, got "${values.method}"`);
  if (method === 'GET' && body !== undefined) fail('A GET request cannot have a body. Use -X POST, or drop --data.');

  let url;
  try {
    url = new URL(positionals[0]).toString();
  } catch {
    fail(`Not a valid URL: ${positionals[0]}`);
  }

  return {
    url,
    headers,
    body,
    method,
    lastEventId: values['last-event-id'] ?? '',
    insecure: values.insecure,
    reconnect: !values['no-reconnect'],
    retryMs: toNumber('retry', values.retry),
    maxRetryMs: toNumber('max-retry', values['max-retry']),
    maxAttempts: toNumber('max-attempts', values['max-attempts']),
    idleTimeoutMs: (toNumber('idle-timeout', values['idle-timeout']) ?? 0) * 1000,
    events: new Set(values.event),
    json: values.json,
    pretty: values.pretty,
    dataOnly: values['data-only'],
    raw: values.raw,
    comments: values.comments,
    maxEvents: toNumber('max-events', values['max-events']),
    quiet: values.quiet,
  };
}

const colourOut = process.stdout.isTTY && !process.env.NO_COLOR;
const colourErr = process.stderr.isTTY && !process.env.NO_COLOR;
const paint = (on, code, s) => (on ? `\x1b[${code}m${s}\x1b[0m` : s);

function formatData(data, pretty) {
  if (!pretty) return data;
  try {
    return JSON.stringify(JSON.parse(data), null, 2);
  } catch {
    return data;
  }
}

function printEvent(evt, opts, out) {
  if (opts.json) {
    let data = evt.data;
    if (opts.pretty) {
      try { data = JSON.parse(evt.data); } catch { /* keep as string */ }
    }
    out(JSON.stringify({ ts: new Date().toISOString(), id: evt.id, event: evt.event, data }) + '\n');
    return;
  }
  if (opts.dataOnly) {
    out(formatData(evt.data, opts.pretty) + '\n');
    return;
  }
  const ts = new Date().toLocaleTimeString('en-IN', { hour12: false });
  const idPart = evt.id ? paint(colourOut, '2', ` id=${evt.id}`) : '';
  out(`${paint(colourOut, '2', ts)} ${paint(colourOut, '36', evt.event)}${idPart}\n${formatData(evt.data, opts.pretty)}\n\n`);
}

/**
 * Connects and keeps streaming until the signal aborts, max-events is reached,
 * reconnect is off and the stream ends, or a non-retryable error happens.
 * Returns { events, exitCode }.
 */
export async function run(opts, { signal, out = (s) => process.stdout.write(s), err = (s) => process.stderr.write(s) } = {}) {
  const log = (msg) => { if (!opts.quiet) err(paint(colourErr, '33', `[sse] ${msg}`) + '\n'); };

  if (opts.insecure) process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

  let lastEventId = opts.lastEventId;
  let retryMs = opts.retryMs;
  let failures = 0;
  let events = 0;
  let stopped = signal?.aborted ?? false;

  signal?.addEventListener('abort', () => { stopped = true; }, { once: true });

  const sleep = (ms) => new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => { clearTimeout(t); resolve(); }, { once: true });
  });

  while (!stopped) {
    const attempt = new AbortController();
    const onStop = () => attempt.abort();
    signal?.addEventListener('abort', onStop, { once: true });

    let idleTimer;
    const touchIdle = () => {
      if (!opts.idleTimeoutMs) return;
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => attempt.abort(new Error(`no data for ${opts.idleTimeoutMs / 1000}s`)), opts.idleTimeoutMs);
    };

    let gotEvent = false;
    try {
      const headers = { Accept: 'text/event-stream', 'Cache-Control': 'no-cache', ...opts.headers };
      if (lastEventId) headers['Last-Event-ID'] = lastEventId;

      log(`connecting ${opts.method} ${opts.url}${lastEventId ? ` (Last-Event-ID: ${lastEventId})` : ''}`);
      touchIdle();
      const res = await fetch(opts.url, { method: opts.method, headers, body: opts.body, signal: attempt.signal });

      if (res.status === 204) {
        log('server replied 204 No Content, which means "stop reconnecting"');
        return { events, exitCode: 0 };
      }
      if (!res.ok) {
        const text = (await res.text().catch(() => '')).slice(0, 500);
        log(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ''}`);
        const retryable = res.status >= 500 || res.status === 408 || res.status === 429;
        if (!retryable) {
          log('not retrying a client error (4xx)');
          return { events, exitCode: 1 };
        }
        throw new Error(`HTTP ${res.status}`);
      }
      const type = res.headers.get('content-type') ?? '';
      if (!type.includes('text/event-stream')) log(`warning: Content-Type is "${type}", not text/event-stream`);
      log(`connected (HTTP ${res.status})`);
      failures = 0;

      const parser = new SseParser({
        onLine: opts.raw ? (line) => out(line + '\n') : undefined,
        onComment: (c) => { if (opts.comments && !opts.raw && !opts.json) out(paint(colourOut, '2', `: ${c}`) + '\n'); },
        onRetry: (ms) => { retryMs = ms; },
        onEvent: (evt) => {
          gotEvent = true;
          lastEventId = evt.id;
          if (opts.events.size && !opts.events.has(evt.event)) return;
          events++;
          if (!opts.raw) printEvent(evt, opts, out);
          if (opts.maxEvents && events >= opts.maxEvents) {
            stopped = true;
            attempt.abort();
          }
        },
      });

      const decoder = new TextDecoder('utf-8');
      for await (const chunk of res.body) {
        touchIdle();
        parser.feed(decoder.decode(chunk, { stream: true }));
        if (stopped) break;
      }
      if (!stopped) log('stream ended by server');
    } catch (e) {
      if (stopped) break;
      const reason = attempt.signal.reason instanceof Error ? attempt.signal.reason.message : (e.cause?.message ?? e.message);
      log(`connection error: ${reason}`);
    } finally {
      clearTimeout(idleTimer);
      signal?.removeEventListener('abort', onStop);
    }

    if (stopped) break;
    if (!opts.reconnect) return { events, exitCode: 0 };

    if (!gotEvent) failures++;
    if (opts.maxAttempts && failures >= opts.maxAttempts) {
      log(`giving up after ${failures} failed attempt(s) in a row`);
      return { events, exitCode: 1 };
    }
    // Back off on repeated failures: retry, 2x, 4x … capped at max-retry.
    const delay = Math.min(retryMs * 2 ** Math.max(0, failures - 1), opts.maxRetryMs);
    log(`reconnecting in ${delay} ms`);
    await sleep(delay);
  }
  return { events, exitCode: 0 };
}

function isMainModule() {
  if (!process.argv[1]) return false;
  try {
    return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}

if (isMainModule()) {
  const opts = parseCli(process.argv.slice(2));
  const stop = new AbortController();
  let interrupts = 0;
  process.on('SIGINT', () => {
    if (++interrupts > 1) process.exit(130);
    stop.abort();
  });
  process.on('SIGTERM', () => stop.abort());
  process.stdout.on('error', (e) => { if (e.code === 'EPIPE') process.exit(0); });
  const { events, exitCode } = await run(opts, { signal: stop.signal });
  if (!opts.quiet) process.stderr.write(paint(colourErr, '33', `[sse] done, ${events} event(s)`) + '\n');
  process.exitCode = exitCode;
}
