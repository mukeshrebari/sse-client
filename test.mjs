// Run with: node --test
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { SseParser, run } from './sse.mjs';

function collect(chunks) {
  const events = [];
  const comments = [];
  let retry;
  const p = new SseParser({ onEvent: (e) => events.push(e), onComment: (c) => comments.push(c), onRetry: (ms) => { retry = ms; } });
  for (const c of chunks) p.feed(c);
  return { events, comments, retry };
}

test('parser: basic, multi-line data, named event, id, retry, comment', () => {
  const { events, comments, retry } = collect([
    ': hello\n',
    'data: one\n\n',
    'event: update\nid: 7\ndata: a\ndata: b\n\n',
    'retry: 1500\n\n',
  ]);
  assert.deepEqual(comments, ['hello']);
  assert.deepEqual(events, [
    { id: '', event: 'message', data: 'one' },
    { id: '7', event: 'update', data: 'a\nb' },
  ]);
  assert.equal(retry, 1500);
});

test('parser: CRLF and CR line endings, split across chunks, BOM', () => {
  const { events } = collect(['﻿data: x\r', '\n\r', '\ndata: y\r\r', 'data:no-space\n', '\n']);
  assert.deepEqual(events.map((e) => e.data), ['x', 'y', 'no-space']);
});

test('parser: event with no data is not dispatched; id persists', () => {
  const { events } = collect(['id: 5\nevent: ping\n\n', 'data: z\n\n']);
  assert.deepEqual(events, [{ id: '5', event: 'message', data: 'z' }]);
});

function startServer(handler) {
  return new Promise((resolve) => {
    const server = createServer(handler);
    server.listen(0, '127.0.0.1', () => resolve(server));
  });
}

const baseOpts = (url, extra = {}) => ({
  url, headers: {}, body: undefined, method: 'GET', lastEventId: '', insecure: false,
  reconnect: true, retryMs: 50, maxRetryMs: 200, maxAttempts: undefined, idleTimeoutMs: 0,
  events: new Set(), json: true, pretty: false, dataOnly: false, raw: false, comments: false,
  maxEvents: undefined, quiet: true, ...extra,
});

test('GET with custom headers, reconnect resumes with Last-Event-ID', async () => {
  const seen = [];
  let n = 0;
  const server = await startServer((req, res) => {
    seen.push({ method: req.method, auth: req.headers.authorization, last: req.headers['last-event-id'] });
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    n++;
    res.end(`retry: 20\nid: ${n}\ndata: hit ${n}\n\n`); // Ends each time, forcing a reconnect.
  });
  const url = `http://127.0.0.1:${server.address().port}/s`;
  const lines = [];
  const result = await run(baseOpts(url, { headers: { Authorization: 'Bearer t' }, maxEvents: 3 }), { out: (s) => lines.push(s) });
  server.close();

  assert.equal(result.events, 3);
  assert.deepEqual(lines.map((l) => JSON.parse(l).data), ['hit 1', 'hit 2', 'hit 3']);
  assert.deepEqual(seen.map((s) => s.method), ['GET', 'GET', 'GET']);
  assert.ok(seen.every((s) => s.auth === 'Bearer t'));
  assert.deepEqual(seen.map((s) => s.last), [undefined, '1', '2']);
});

test('POST sends body and headers; --no-reconnect exits when stream ends', async () => {
  let got;
  const server = await startServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      got = { method: req.method, type: req.headers['content-type'], key: req.headers['x-api-key'], body };
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('event: token\ndata: Hel\n\n');
      res.end('event: token\ndata: lo\n\nevent: done\ndata: [DONE]\n\n');
    });
  });
  const url = `http://127.0.0.1:${server.address().port}/chat`;
  const lines = [];
  const result = await run(baseOpts(url, {
    method: 'POST', body: '{"q":"hi"}', reconnect: false, events: new Set(['token']),
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'k1' },
  }), { out: (s) => lines.push(s) });
  server.close();

  assert.deepEqual(got, { method: 'POST', type: 'application/json', key: 'k1', body: '{"q":"hi"}' });
  assert.equal(result.events, 2); // "done" filtered out by --event token
  assert.deepEqual(lines.map((l) => JSON.parse(l).data), ['Hel', 'lo']);
});

test('4xx stops without retrying; 204 stops cleanly', async () => {
  let calls = 0;
  const server = await startServer((req, res) => {
    calls++;
    res.writeHead(req.url === '/gone' ? 204 : 401);
    res.end();
  });
  const port = server.address().port;
  const r401 = await run(baseOpts(`http://127.0.0.1:${port}/x`), { out: () => {} });
  const r204 = await run(baseOpts(`http://127.0.0.1:${port}/gone`), { out: () => {} });
  server.close();
  assert.equal(r401.exitCode, 1);
  assert.equal(r204.exitCode, 0);
  assert.equal(calls, 2);
});

test('5xx retries and gives up after --max-attempts', async () => {
  let calls = 0;
  const server = await startServer((req, res) => { calls++; res.writeHead(503); res.end(); });
  const r = await run(baseOpts(`http://127.0.0.1:${server.address().port}/`, { maxAttempts: 3 }), { out: () => {} });
  server.close();
  assert.equal(r.exitCode, 1);
  assert.equal(calls, 3);
});

test('idle timeout forces a reconnect on a silent stream', async () => {
  let calls = 0;
  const server = await startServer((req, res) => {
    calls++;
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    if (calls === 1) res.write('data: first\n\n'); // then go silent
    else res.end('data: second\n\n');
  });
  const lines = [];
  const r = await run(baseOpts(`http://127.0.0.1:${server.address().port}/`, { idleTimeoutMs: 150, maxEvents: 2 }), { out: (s) => lines.push(s) });
  server.closeAllConnections();
  server.close();
  assert.equal(r.events, 2);
  assert.equal(calls, 2);
});
