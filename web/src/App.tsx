import { useCallback, useEffect, useRef, useState } from 'react';

// Real-Time Event Monitoring channels. Values map to /event/<name> on the
// back-end. Kept alphabetical for easy scanning in the dropdown.
const CHANNELS = [
  'ApiAnomalyEvent',
  'ApiEventStream',
  'BulkApiResultEvent',
  'ConcurLongRunApexErrEvent',
  'CredentialStuffingEvent',
  'FileEvent',
  'GuestUserAnomalyEvent',
  'LightningUriEventStream',
  'ListViewEventStream',
  'LoginAnomalyEvent',
  'LoginAsEventStream',
  'LoginEventStream',
  'LogoutEventStream',
  'PermissionSetEvent',
  'ReportAnomalyEvent',
  'ReportEventStream',
  'SessionHijackingEvent',
  'UniversalAnomalyEvent',
  'UriEventStream',
] as const;

type ConnState =
  | { state: 'disconnected' }
  | { state: 'connecting'; channels?: string[] }
  | { state: 'connected'; channels?: string[]; instanceUrl?: string }
  | { state: 'error'; channels?: string[]; message: string };

interface SfMessage {
  event?: { replayId?: number };
  payload?: Record<string, unknown>;
  [key: string]: unknown;
}

/** Transaction Security Policy annotation added by the back-end. */
interface TspInfo {
  policyId: string;
  policyName?: string;
  eventDate?: string;
}

/** Format an ISO timestamp for display; fall back to the raw string. */
function formatEventDate(iso?: string): string {
  if (!iso) return 'Unknown';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** SSE envelope: the source channel, raw event, and any TSP annotation. */
interface EnrichedMessage {
  channel: string;
  raw: SfMessage;
  tsp: TspInfo | null;
}

export function App() {
  const [myDomain, setMyDomain] = useState('');
  const [consumerKey, setConsumerKey] = useState('');
  const [consumerSecret, setConsumerSecret] = useState('');
  // Multiple channels can be selected at once.
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // Replay mode: '-1' new only, '-2' all retained, 'specific' + a replay ID.
  const [replayMode, setReplayMode] = useState<'-1' | '-2' | 'specific'>('-1');
  const [replayId, setReplayId] = useState('');

  const [infoOpen, setInfoOpen] = useState(false);

  const [status, setStatus] = useState<ConnState>({ state: 'disconnected' });
  const [count, setCount] = useState(0);
  // Per-channel message counts, keyed by channel name.
  const [countsByChannel, setCountsByChannel] = useState<
    Record<string, number>
  >({});
  const [lastMessage, setLastMessage] = useState<SfMessage | null>(null);
  const [lastChannel, setLastChannel] = useState<string | null>(null);
  const [lastTsp, setLastTsp] = useState<TspInfo | null>(null);
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  const esRef = useRef<EventSource | null>(null);

  // Open the SSE stream once on mount; it persists across connect/disconnect.
  useEffect(() => {
    const es = new EventSource('/api/stream');
    esRef.current = es;

    es.addEventListener('sf-message', (e) => {
      const data = JSON.parse((e as MessageEvent).data) as EnrichedMessage;
      setLastMessage(data.raw);
      setLastChannel(data.channel);
      setLastTsp(data.tsp);
      setCount((c) => c + 1);
      setCountsByChannel((prev) => ({
        ...prev,
        [data.channel]: (prev[data.channel] ?? 0) + 1,
      }));
    });
    es.addEventListener('status', (e) => {
      setStatus(JSON.parse((e as MessageEvent).data) as ConnState);
    });

    return () => es.close();
  }, []);

  const connect = useCallback(async () => {
    setFormError(null);

    const channels = CHANNELS.filter((c) => selected.has(c));
    if (channels.length === 0) {
      setFormError('Select at least one channel to listen to.');
      return;
    }

    let replay: number;
    if (replayMode === 'specific') {
      const parsed = Number(replayId);
      if (!replayId.trim() || !Number.isInteger(parsed) || parsed < 0) {
        setFormError('Replay ID must be a non-negative integer.');
        return;
      }
      replay = parsed;
    } else {
      replay = Number(replayMode); // -1 or -2
    }

    setBusy(true);
    try {
      const res = await fetch('/api/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          myDomain,
          consumerKey,
          consumerSecret,
          channels,
          replayId: replay,
        }),
      });
      const json = await res.json();
      if (!res.ok || !json.ok) {
        throw new Error(json.error ?? `Request failed (${res.status})`);
      }
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }, [myDomain, consumerKey, consumerSecret, selected, replayMode, replayId]);

  const disconnect = useCallback(async () => {
    setBusy(true);
    try {
      await fetch('/api/disconnect', { method: 'POST' });
    } finally {
      setBusy(false);
    }
  }, []);

  const resetCounter = useCallback(() => {
    setCount(0);
    setCountsByChannel({});
    setLastMessage(null);
    setLastChannel(null);
    setLastTsp(null);
  }, []);

  const isConnected = status.state === 'connected';
  const isConnecting = status.state === 'connecting' || busy;

  const toggleChannel = useCallback(
    (name: string) => {
      if (isConnected) return;
      setSelected((prev) => {
        const next = new Set(prev);
        if (next.has(name)) next.delete(name);
        else next.add(name);
        return next;
      });
    },
    [isConnected],
  );

  const selectAll = useCallback(
    () => setSelected(new Set(CHANNELS)),
    [],
  );
  const clearAll = useCallback(() => setSelected(new Set()), []);

  return (
    <div className="app">
      <header>
        <div className="title-row">
          <h1>Salesforce Streaming Monitor</h1>
          <button
            type="button"
            className="info-toggle"
            aria-expanded={infoOpen}
            aria-controls="info-pane"
            title="About this application"
            onClick={() => setInfoOpen((o) => !o)}
          >
            ⓘ
          </button>
        </div>
        <StatusBadge status={status} />
      </header>

      {infoOpen && (
        <section className="panel info-pane" id="info-pane">
          <h2>About</h2>
          <p>
            This application authenticates to Salesforce using the OAuth 2.0
            Client Credentials Flow, subscribes to one or more Real-Time Event
            Monitoring streaming channels, and displays a live message counter
            and the content of the last event received. When an event carries a
            Transaction Security Policy (a non-null <code>PolicyId</code>), it
            raises an alert showing the policy name and event date.
          </p>
          <dl className="info-meta">
            <dt>Built by</dt>
            <dd>Jérôme CHENUT — Senior Program Architect, Salesforce</dd>
            <dt>Contact</dt>
            <dd>
              <a href="mailto:jchenut@salesforce.com">jchenut@salesforce.com</a>
            </dd>
            <dt>Version</dt>
            <dd>v1</dd>
          </dl>
        </section>
      )}

      <section className="panel">
        <h2>Connection</h2>
        <div className="form-grid">
          <label>
            My Domain URL
            <input
              type="text"
              placeholder="https://mycompany.my.salesforce.com"
              value={myDomain}
              onChange={(e) => setMyDomain(e.target.value)}
              disabled={isConnected}
              autoComplete="off"
            />
          </label>
          <label>
            Consumer Key
            <input
              type="text"
              value={consumerKey}
              onChange={(e) => setConsumerKey(e.target.value)}
              disabled={isConnected}
              autoComplete="off"
            />
          </label>
          <label>
            Consumer Secret
            <input
              type="password"
              value={consumerSecret}
              onChange={(e) => setConsumerSecret(e.target.value)}
              disabled={isConnected}
              autoComplete="off"
            />
          </label>
          <label>
            Replay
            <select
              value={replayMode}
              onChange={(e) =>
                setReplayMode(e.target.value as '-1' | '-2' | 'specific')
              }
              disabled={isConnected}
            >
              <option value="-1">New events only (-1)</option>
              <option value="-2">All retained events (-2)</option>
              <option value="specific">From a specific replay ID…</option>
            </select>
          </label>
          {replayMode === 'specific' && (
            <label>
              Replay ID
              <input
                type="number"
                min={0}
                placeholder="e.g. 1024"
                value={replayId}
                onChange={(e) => setReplayId(e.target.value)}
                disabled={isConnected}
              />
            </label>
          )}
        </div>

        <div className="channels">
          <div className="channels-head">
            <span className="channels-title">
              Channels{' '}
              <span className="muted">({selected.size} selected)</span>
            </span>
            {!isConnected && (
              <span className="channels-bulk">
                <button className="ghost small" type="button" onClick={selectAll}>
                  Select all
                </button>
                <button className="ghost small" type="button" onClick={clearAll}>
                  Clear
                </button>
              </span>
            )}
          </div>
          <div className="channel-grid">
            {CHANNELS.map((c) => (
              <label key={c} className="channel-item">
                <input
                  type="checkbox"
                  checked={selected.has(c)}
                  onChange={() => toggleChannel(c)}
                  disabled={isConnected}
                />
                <span>{c}</span>
                {countsByChannel[c] ? (
                  <span className="channel-count">{countsByChannel[c]}</span>
                ) : null}
              </label>
            ))}
          </div>
        </div>

        <div className="actions">
          {!isConnected ? (
            <button onClick={connect} disabled={isConnecting}>
              {isConnecting ? 'Connecting…' : 'Connect'}
            </button>
          ) : (
            <button className="secondary" onClick={disconnect} disabled={busy}>
              Disconnect
            </button>
          )}
        </div>

        {formError && <p className="error">{formError}</p>}
        {status.state === 'error' && (
          <p className="error">{status.message}</p>
        )}
      </section>

      <section className="panel">
        <div className="counter-row">
          <div className="counter">
            <span className="counter-value">{count}</span>
            <span className="counter-label">messages received</span>
          </div>
          <button className="ghost" onClick={resetCounter}>
            Reset
          </button>
        </div>
      </section>

      {lastTsp && (
        <section className="panel alert-tsp" role="alert">
          <h2>⚠ Transaction Security Policy fired</h2>
          <p className="tsp-name">
            {lastTsp.policyName ?? '(name could not be resolved)'}
          </p>
          <p className="tsp-date">
            <span className="tsp-date-label">Event date</span>
            {formatEventDate(lastTsp.eventDate)}
          </p>
          <p className="tsp-id muted">Policy ID: {lastTsp.policyId}</p>
        </section>
      )}

      <section className="panel">
        <div className="msg-head">
          <h2>Last message</h2>
          {lastChannel && <span className="channel-tag">{lastChannel}</span>}
        </div>
        {lastMessage ? (
          <pre className="message">{JSON.stringify(lastMessage, null, 2)}</pre>
        ) : (
          <p className="muted">No messages received yet.</p>
        )}
      </section>
    </div>
  );
}

function StatusBadge({ status }: { status: ConnState }) {
  const count =
    (status.state === 'connected' || status.state === 'connecting') &&
    status.channels
      ? status.channels.length
      : 0;
  const label =
    status.state === 'connected'
      ? `Connected · ${count} channel${count === 1 ? '' : 's'}`
      : status.state === 'connecting'
        ? 'Connecting…'
        : status.state === 'error'
          ? 'Error'
          : 'Disconnected';
  return <span className={`badge badge-${status.state}`}>{label}</span>;
}
