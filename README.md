# Salesforce Streaming Monitor

A web app that authenticates to Salesforce using the **OAuth 2.0 Client
Credentials Flow** and listens to **one or more Platform Event / Real-Time
Event Monitoring** streaming channels at once via the CometD (Bayeux) Streaming
API. Incoming events are relayed to the browser over Server-Sent Events (SSE);
the UI shows a total message counter, a per-channel breakdown, and the content
of the last message (tagged with the channel it arrived on).

## Architecture

```
Browser (React/Vite)  ⇄  Back-end (Fastify/TS)  ⇄  Salesforce
   dropdown, counter,     - CCF token exchange       - OAuth token endpoint
   last-message panel     - CometD subscription       - Platform Events
        ▲   SSE           - SSE relay + reconnect      (CometD long-poll)
```

The browser **cannot** subscribe to CometD directly (CORS + token exposure), so
the back-end is the CometD client and relays events to the browser via SSE.
Credentials are held **in memory only** on the back-end — never persisted, never
returned to the browser.

## Prerequisites (Salesforce side)

1. **Connected App** with OAuth enabled and the **Client Credentials Flow**
   turned on (set a "Run As" user under *Manage → OAuth Policies*).
2. The consumer **Key** and **Secret** from that Connected App.
3. Your **My Domain** URL, e.g. `https://mycompany.my.salesforce.com`.
4. **Real-Time Event Monitoring**: the channel dropdown lists the full set of
   RTEM events (see below). They require the **Shield / Event Monitoring**
   add-on license and the run-as user needs the *View Real-Time Event
   Monitoring Data* perm. Without this, the subscription succeeds but no events
   ever arrive.
5. **Transaction Security Policy detection**: when an event's payload carries a
   non-null `PolicyId`, a Transaction Security Policy has fired. The back-end
   queries the `TransactionSecurityPolicy` object **once per connection** (the
   Id→name map is cached) to resolve the policy name, and the UI raises an
   alert showing that name. The run-as user needs read access to
   `TransactionSecurityPolicy` for the name lookup to succeed.

## Channels

The UI offers all 19 Real-Time Event Monitoring channels as a checkbox list —
**select any number of them** and the back-end subscribes to all of the chosen
channels over a single CometD session. Each maps to `/event/<name>`; the same
replay and Transaction Security Policy (`PolicyId`) detection applies to every
one, and each relayed event is tagged with its source channel so the UI can
show a per-channel count.

Two broad categories behave differently:

- **Standard RTEM events** fire on ordinary activity and are the easiest to see:
  `ApiEventStream`, `BulkApiResultEvent`, `ConcurLongRunApexErrEvent`,
  `FileEvent`, `LightningUriEventStream`, `ListViewEventStream`,
  `LoginAsEventStream`, `LoginEventStream`, `LogoutEventStream`,
  `PermissionSetEvent`, `ReportEventStream`, `UriEventStream`.
- **Threat Detection events** only fire when Salesforce's detection models flag
  suspicious activity, so they may stay quiet even with a correct subscription:
  `ApiAnomalyEvent`, `CredentialStuffingEvent`, `GuestUserAnomalyEvent`,
  `LoginAnomalyEvent`, `ReportAnomalyEvent`, `SessionHijackingEvent`,
  `UniversalAnomalyEvent`.

## Run it

**One command (recommended)** — from the repo root, launches back-end + front-end together:

```bash
npm run install:all   # first time only: installs root, server and web deps
npm run dev           # starts server (:3001) and web (:5173) with prefixed logs
```

Use `npm run dev:mock` instead to also enable the mock event endpoint (see below).

**Or two terminals**, if you prefer to run them separately:

```bash
# Terminal 1 — back-end (http://localhost:3001)
cd server && npm install && npm run dev

# Terminal 2 — front-end (http://localhost:5173)
cd web && npm install && npm run dev
```

Open http://localhost:5173, fill in My Domain / Consumer Key / Consumer Secret,
select one or more channels and a **Replay** mode, and click **Connect**.

**Replay modes:**
- **New events only (-1)** — default; only events published after subscribing.
- **All retained events (-2)** — every event still within the retention window.
- **From a specific replay ID** — resume just after a given event's replay ID.

## Testing the SSE relay without a live org

Start the server with `ALLOW_MOCK=1 npm run dev`, then inject a fake event:

```bash
curl -X POST http://localhost:3001/api/mock-event \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"hello":"world"},"event":{"replayId":1}}'
```

The counter should increment and the panel should show the payload.

To exercise the **Transaction Security Policy** alert, include a `PolicyId` in
the payload (and optionally a `mockPolicyName` to simulate the name lookup):

```bash
curl -X POST http://localhost:3001/api/mock-event \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"PolicyId":"0NIxx0000004C93"},"mockPolicyName":"Block Data Export"}'
```

## API

| Method | Path              | Purpose                                  |
| ------ | ----------------- | ---------------------------------------- |
| POST   | `/api/connect`    | `{myDomain,consumerKey,consumerSecret,channels[],replayId?}` → auth + subscribe |
| POST   | `/api/disconnect` | Tear down subscription, clear creds      |
| GET    | `/api/status`     | Current connection status                |
| GET    | `/api/stream`     | SSE stream (`sf-message`, `status` events) |
| POST   | `/api/mock-event` | Inject a fake event (only if `ALLOW_MOCK=1`) |
