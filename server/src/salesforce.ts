import { EventEmitter } from 'node:events';
import { Connection } from 'jsforce';
import { StreamingExtension, type Subscription } from 'jsforce/lib/api/streaming.js';

/** A channel name as shown in the UI dropdown (without the /event/ prefix). */
export type ChannelName = string;

export interface ConnectParams {
  myDomain: string;
  consumerKey: string;
  consumerSecret: string;
  /** One or more channels to subscribe to simultaneously. */
  channels: ChannelName[];
  /**
   * CometD replay ID applied to every channel: -1 = new events only (default),
   * -2 = all retained events, or a specific replay ID to resume from just
   * after that event.
   */
  replayId?: number;
}

export type StreamStatus =
  | { state: 'disconnected' }
  | { state: 'connecting'; channels: ChannelName[] }
  | { state: 'connected'; channels: ChannelName[]; instanceUrl: string }
  | { state: 'error'; channels?: ChannelName[]; message: string };

interface TokenResponse {
  access_token: string;
  instance_url: string;
  token_type: string;
  issued_at?: string;
}

/** Info about a fired Transaction Security Policy, resolved to its name. */
export interface TspInfo {
  policyId: string;
  /** Human-readable policy name, or undefined if the lookup failed. */
  policyName?: string;
  /** ISO-8601 timestamp of the event, from the payload, if present. */
  eventDate?: string;
}

/** Envelope relayed to the browser: the raw event plus any TSP annotation. */
export interface EnrichedMessage {
  /** The channel this event arrived on (without the /event/ prefix). */
  channel: ChannelName;
  raw: unknown;
  tsp: TspInfo | null;
}

/**
 * Manages a single live connection to Salesforce: performs the OAuth 2.0
 * Client Credentials token exchange, opens a CometD subscription to a platform
 * event channel, and re-emits each event to any listeners (the SSE relay).
 *
 * Credentials are held only in memory and are cleared on disconnect. They are
 * retained while connected solely to allow a transparent re-auth if the token
 * expires or the CometD session drops.
 */
export class SalesforceStreamer extends EventEmitter {
  private creds: ConnectParams | null = null;
  private conn: Connection | null = null;
  private subscriptions: Subscription[] = [];
  private status: StreamStatus = { state: 'disconnected' };
  private reauthTimer: NodeJS.Timeout | null = null;
  /**
   * Cache of Transaction Security Policy Id → name, loaded lazily on the first
   * event that carries a PolicyId and reused for the life of the connection.
   */
  private tspPolicyNames: Map<string, string> | null = null;
  private tspLoadPromise: Promise<Map<string, string>> | null = null;

  getStatus(): StreamStatus {
    return this.status;
  }

  /** Full CometD channel path for a platform event. */
  private channelPath(channel: ChannelName): string {
    return `/event/${channel}`;
  }

  private normalizeDomain(myDomain: string): string {
    let d = myDomain.trim();
    if (!/^https?:\/\//i.test(d)) d = `https://${d}`;
    return d.replace(/\/+$/, '');
  }

  /** Exchange consumer key/secret for an access token via Client Credentials Flow. */
  private async fetchToken(creds: ConnectParams): Promise<TokenResponse> {
    const base = this.normalizeDomain(creds.myDomain);
    const body = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: creds.consumerKey,
      client_secret: creds.consumerSecret,
    });
    const res = await fetch(`${base}/services/oauth2/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    });
    const text = await res.text();
    if (!res.ok) {
      // Salesforce returns { error, error_description } on failure.
      let detail = text;
      try {
        const j = JSON.parse(text);
        detail = j.error_description || j.error || text;
      } catch {
        /* keep raw text */
      }
      throw new Error(`Token exchange failed (${res.status}): ${detail}`);
    }
    return JSON.parse(text) as TokenResponse;
  }

  /** Connect (or reconnect) using the stored/provided credentials. */
  async connect(params: ConnectParams): Promise<StreamStatus> {
    await this.disconnect();
    this.creds = params;
    return this.open();
  }

  private async open(): Promise<StreamStatus> {
    const creds = this.creds;
    if (!creds) throw new Error('No credentials set');

    this.setStatus({ state: 'connecting', channels: creds.channels });

    const token = await this.fetchToken(creds);

    const conn = new Connection({
      instanceUrl: token.instance_url,
      accessToken: token.access_token,
      // Streaming API version; platform events supported since v37.0.
      version: '60.0',
    });
    this.conn = conn;

    // Replay: -1 = new events only, -2 = all retained, N = resume after N.
    const replayId = creds.replayId ?? -1;

    // Each channel gets its own Replay extension (it is channel-specific); the
    // auth-failure extension is shared and triggers a single re-auth.
    const authFailureExt = new StreamingExtension.AuthFailure(() => {
      this.emit('log', 'CometD auth failure — attempting re-auth');
      void this.reconnectSoon();
    });
    const replayExts = creds.channels.map(
      (channel) =>
        new StreamingExtension.Replay(this.channelPath(channel), replayId),
    );

    const fayeClient = conn.streaming.createClient([
      ...replayExts,
      authFailureExt,
    ]);

    // Subscribe to every requested channel over the same CometD client, tagging
    // each event with the channel it arrived on.
    this.subscriptions = creds.channels.map((channel) => {
      const path = this.channelPath(channel);
      return fayeClient.subscribe(path, (message: unknown) => {
        void this.handleMessage(channel, message);
      });
    });

    this.setStatus({
      state: 'connected',
      channels: creds.channels,
      instanceUrl: token.instance_url,
    });
    this.emit(
      'log',
      `Subscribed to ${creds.channels
        .map((c) => this.channelPath(c))
        .join(', ')} (replayId=${replayId}) on ${token.instance_url}`,
    );
    return this.status;
  }

  /**
   * Enrich an incoming event: if its payload carries a non-null PolicyId, a
   * Transaction Security Policy has fired — resolve its name (cached) and
   * annotate the message before relaying it.
   */
  private async handleMessage(
    channel: ChannelName,
    message: unknown,
  ): Promise<void> {
    let tsp: TspInfo | null = null;
    const policyId = this.extractPolicyId(message);
    if (policyId) {
      let policyName: string | undefined;
      try {
        const names = await this.loadTspPolicyNames();
        policyName = names.get(this.normalizeId(policyId));
      } catch (err) {
        this.emit(
          'log',
          `Failed to resolve TransactionSecurityPolicy name: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
      const eventDate = this.extractEventDate(message);
      tsp = { policyId, policyName, eventDate };
      this.emit(
        'log',
        `Transaction Security Policy fired on ${channel}: ${
          policyName ?? policyId
        }`,
      );
    }
    const enriched: EnrichedMessage = { channel, raw: message, tsp };
    this.emit('message', enriched);
  }

  /** Pull PolicyId out of a CometD event payload, if present and non-null. */
  private extractPolicyId(message: unknown): string | null {
    if (typeof message !== 'object' || message === null) return null;
    const payload = (message as { payload?: Record<string, unknown> }).payload;
    const value = payload?.PolicyId;
    if (typeof value === 'string' && value.trim() !== '') return value;
    return null;
  }

  /**
   * Pull the event timestamp from a CometD payload. RTEM events expose an
   * `EventDate`; platform events also carry a `CreatedDate` — either works.
   */
  private extractEventDate(message: unknown): string | undefined {
    if (typeof message !== 'object' || message === null) return undefined;
    const payload = (message as { payload?: Record<string, unknown> }).payload;
    for (const key of ['EventDate', 'CreatedDate'] as const) {
      const value = payload?.[key];
      if (typeof value === 'string' && value.trim() !== '') return value;
    }
    return undefined;
  }

  /**
   * Salesforce IDs come in 15-char (case-sensitive) and 18-char (case-safe)
   * forms. Normalize to the 15-char prefix so lookups match regardless of the
   * form the event and the query return.
   */
  private normalizeId(id: string): string {
    return id.slice(0, 15);
  }

  /** Load and cache the Id→name map for all Transaction Security Policies. */
  private loadTspPolicyNames(): Promise<Map<string, string>> {
    if (this.tspPolicyNames) return Promise.resolve(this.tspPolicyNames);
    if (this.tspLoadPromise) return this.tspLoadPromise;
    const conn = this.conn;
    if (!conn) return Promise.reject(new Error('Not connected'));

    this.tspLoadPromise = (async () => {
      const result = await conn.query<{
        Id: string;
        DeveloperName: string;
        MasterLabel: string;
      }>('SELECT Id, DeveloperName, MasterLabel FROM TransactionSecurityPolicy');
      const map = new Map<string, string>();
      for (const rec of result.records) {
        map.set(this.normalizeId(rec.Id), rec.MasterLabel || rec.DeveloperName);
      }
      this.tspPolicyNames = map;
      this.emit('log', `Loaded ${map.size} Transaction Security Policies`);
      return map;
    })();
    // Clear the in-flight promise on failure so a later event can retry.
    this.tspLoadPromise.catch(() => {
      this.tspLoadPromise = null;
    });
    return this.tspLoadPromise;
  }

  private reconnectSoon(): void {
    if (this.reauthTimer) return;
    this.reauthTimer = setTimeout(() => {
      this.reauthTimer = null;
      this.open().catch((err) => {
        this.setStatus({
          state: 'error',
          channels: this.creds?.channels,
          message: err instanceof Error ? err.message : String(err),
        });
      });
    }, 1000);
  }

  async disconnect(): Promise<void> {
    if (this.reauthTimer) {
      clearTimeout(this.reauthTimer);
      this.reauthTimer = null;
    }
    for (const sub of this.subscriptions) {
      try {
        sub.cancel();
      } catch {
        /* ignore */
      }
    }
    this.subscriptions = [];
    this.conn = null;
    this.creds = null;
    this.tspPolicyNames = null;
    this.tspLoadPromise = null;
    this.setStatus({ state: 'disconnected' });
  }

  private setStatus(status: StreamStatus): void {
    this.status = status;
    this.emit('status', status);
  }
}
