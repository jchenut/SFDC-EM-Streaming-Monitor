import Fastify from 'fastify';
import cors from '@fastify/cors';
import { SalesforceStreamer, type ConnectParams } from './salesforce.js';

const PORT = Number(process.env.PORT ?? 3001);
const HOST = process.env.HOST ?? '0.0.0.0';
const ALLOW_MOCK = process.env.ALLOW_MOCK === '1';

const app = Fastify({ logger: true });
await app.register(cors, { origin: true });

// Single shared connection for this demo app (in-memory only).
const streamer = new SalesforceStreamer();

streamer.on('log', (msg: string) => app.log.info({ sf: msg }, 'salesforce'));

/** Set of currently-open SSE client responses. */
const sseClients = new Set<import('node:http').ServerResponse>();

function broadcast(event: string, data: unknown): void {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) res.write(payload);
}

streamer.on('message', (msg) => broadcast('sf-message', msg));
streamer.on('status', (status) => broadcast('status', status));

app.post<{ Body: ConnectParams }>('/api/connect', async (req, reply) => {
  const { myDomain, consumerKey, consumerSecret, channels, replayId } =
    req.body ?? {};
  if (!myDomain || !consumerKey || !consumerSecret) {
    return reply.status(400).send({
      error: 'myDomain, consumerKey and consumerSecret are required',
    });
  }
  if (!Array.isArray(channels) || channels.length === 0) {
    return reply.status(400).send({
      error: 'channels must be a non-empty array of channel names',
    });
  }
  if (replayId !== undefined && !Number.isInteger(replayId)) {
    return reply.status(400).send({
      error: 'replayId must be an integer (-1, -2, or a specific replay ID)',
    });
  }
  try {
    const status = await streamer.connect({
      myDomain,
      consumerKey,
      consumerSecret,
      channels,
      replayId,
    });
    return reply.send({ ok: true, status });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return reply.status(502).send({ ok: false, error: message });
  }
});

app.post('/api/disconnect', async (_req, reply) => {
  await streamer.disconnect();
  return reply.send({ ok: true });
});

app.get('/api/status', async (_req, reply) => {
  return reply.send(streamer.getStatus());
});

// Server-Sent Events relay: browser subscribes here to receive events.
app.get('/api/stream', (req, reply) => {
  reply.raw.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  reply.raw.write(`event: status\ndata: ${JSON.stringify(streamer.getStatus())}\n\n`);

  sseClients.add(reply.raw);
  const keepAlive = setInterval(() => reply.raw.write(': keep-alive\n\n'), 25_000);

  req.raw.on('close', () => {
    clearInterval(keepAlive);
    sseClients.delete(reply.raw);
  });
});

// Test-only endpoint: inject a fake event to verify the SSE relay end-to-end.
// Disabled unless ALLOW_MOCK=1 so it can never be reached in a real deployment.
if (ALLOW_MOCK) {
  app.post('/api/mock-event', async (req, reply) => {
    // Mimic the enriched envelope the real streaming path emits. If the mock
    // payload carries a PolicyId, surface it as a fired TSP; an optional
    // `mockPolicyName` lets you exercise the resolved-name display path.
    const raw = (req.body ?? { mock: true }) as {
      payload?: Record<string, unknown>;
      mockPolicyName?: string;
      mockChannel?: string;
    };
    const channel = raw.mockChannel ?? 'MockEventStream';
    const policyId = raw.payload?.PolicyId;
    const eventDate = raw.payload?.EventDate ?? raw.payload?.CreatedDate;
    const tsp =
      typeof policyId === 'string' && policyId.trim() !== ''
        ? {
            policyId,
            policyName: raw.mockPolicyName,
            eventDate: typeof eventDate === 'string' ? eventDate : undefined,
          }
        : null;
    broadcast('sf-message', { channel, raw, tsp });
    return reply.send({ ok: true });
  });
  app.log.warn('Mock event endpoint enabled at POST /api/mock-event');
}

app
  .listen({ port: PORT, host: HOST })
  .then((addr) => app.log.info(`Server listening at ${addr}`))
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });
