import { z } from 'zod';

export interface CorrelationEvent {
  source: 'github' | 'deployment';
  id: string;
  title: string;
  occurredAt: string;
  url?: string;
  /** External text is data, not instructions for the model to follow. */
  untrusted: true;
}

export interface CorrelationResult {
  status: 'not_configured' | 'ok' | 'unavailable';
  events: CorrelationEvent[];
  message: string;
}

export interface CorrelationOptions {
  githubRepository?: string | undefined;
  githubToken?: string | undefined;
  deploymentSourceUrl?: string | undefined;
  fetchImpl?: typeof fetch | undefined;
}

const commitListSchema = z
  .array(
    z.object({
      sha: z.string(),
      html_url: z.string().optional(),
      commit: z.object({
        message: z.string(),
        author: z
          .object({
            date: z.string().optional(),
          })
          .nullable()
          .optional(),
      }),
    }),
  )
  .max(30);

const githubDeploymentSchema = z
  .array(
    z.object({
      id: z.number(),
      environment: z.string().optional(),
      description: z.string().nullable().optional(),
      sha: z.string().optional(),
      created_at: z.string(),
    }),
  )
  .max(30);

const deploymentFeedSchema = z
  .array(
    z.object({
      id: z.union([z.string(), z.number()]),
      service: z.string(),
      version: z.string(),
      deployedAt: z.string(),
      url: z.string().optional(),
    }),
  )
  .max(50);

const MAX_EVENTS = 20;
const MAX_BODY_BYTES = 1_000_000;
const MAX_TITLE = 200;
const MAX_URL = 2_000;

/**
 * Read-only correlation against GitHub and an optional deployment feed.
 * Failures stay inside the result. Diagnosis still returns.
 */
export class CorrelationService {
  constructor(private readonly options: CorrelationOptions = {}) {}

  async correlate(window: { since: string; until: string }): Promise<CorrelationResult> {
    const repository = this.options.githubRepository;
    const deploymentSourceUrl = this.options.deploymentSourceUrl;
    if (repository === undefined && deploymentSourceUrl === undefined) {
      return {
        status: 'not_configured',
        events: [],
        message: 'GitHub repository and deployment source are not configured.',
      };
    }

    const events: CorrelationEvent[] = [];
    const problems: string[] = [];
    if (repository !== undefined) {
      try {
        events.push(...(await this.fetchGithub(repository, window)));
      } catch (err) {
        problems.push(this.safeMessage(err));
      }
    }
    if (deploymentSourceUrl !== undefined) {
      try {
        events.push(...(await this.fetchDeploymentFeed(deploymentSourceUrl, window)));
      } catch (err) {
        problems.push(this.safeMessage(err));
      }
    }

    const capped = events
      .filter((event) => inWindow(event.occurredAt, window))
      .sort((a, b) => b.occurredAt.localeCompare(a.occurredAt))
      .slice(0, MAX_EVENTS);
    if (problems.length > 0 && capped.length === 0) {
      return { status: 'unavailable', events: [], message: problems.join('; ') };
    }
    const partial = problems.length > 0 ? ` Partial failure: ${problems.join('; ')}` : '';
    return {
      status: 'ok',
      events: capped,
      message:
        capped.length === 0
          ? `No matching changes were found in the observation window.${partial}`
          : `Correlation events are nearby changes, not causes.${partial}`,
    };
  }

  private async fetchGithub(
    repository: string,
    window: { since: string; until: string },
  ): Promise<CorrelationEvent[]> {
    const [owner, repo] = repository.split('/');
    if (owner === undefined || repo === undefined || owner.length === 0 || repo.length === 0) {
      throw new Error('GITHUB_REPOSITORY must be owner/name');
    }
    const headers = this.githubHeaders();
    const commitsUrl = new URL(
      `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/commits`,
    );
    commitsUrl.searchParams.set('since', window.since);
    commitsUrl.searchParams.set('until', window.until);
    commitsUrl.searchParams.set('per_page', '10');
    const commits = commitListSchema.parse(await this.fetchJson(commitsUrl, headers));
    const events: CorrelationEvent[] = commits.map((commit) => {
      const title = commit.commit.message.split('\n')[0] ?? commit.sha;
      const event: CorrelationEvent = {
        source: 'github',
        id: safeExternalText(commit.sha.slice(0, 12), 120),
        title: safeExternalText(title, MAX_TITLE),
        occurredAt: commit.commit.author?.date ?? window.until,
        untrusted: true,
      };
      const url = safeExternalUrl(commit.html_url);
      if (url !== undefined) event.url = url;
      return event;
    });

    const deploymentsUrl = new URL(
      `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/deployments`,
    );
    deploymentsUrl.searchParams.set('per_page', '10');
    const deployments = githubDeploymentSchema.parse(await this.fetchJson(deploymentsUrl, headers));
    for (const deployment of deployments) {
      const title =
        deployment.description ?? deployment.environment ?? deployment.sha ?? 'deployment';
      events.push({
        source: 'github',
        id: `deployment-${deployment.id}`,
        title: safeExternalText(title, MAX_TITLE),
        occurredAt: deployment.created_at,
        untrusted: true,
      });
    }
    return events;
  }

  private async fetchDeploymentFeed(
    sourceUrl: string,
    window: { since: string; until: string },
  ): Promise<CorrelationEvent[]> {
    const url = new URL(sourceUrl);
    url.searchParams.set('since', window.since);
    url.searchParams.set('until', window.until);
    const payload = deploymentFeedSchema.parse(
      await this.fetchJson(url, { accept: 'application/json' }),
    );
    return payload.map((deployment) => {
      const event: CorrelationEvent = {
        source: 'deployment',
        id: safeExternalText(String(deployment.id), 120),
        title: safeExternalText(`${deployment.service} ${deployment.version}`, MAX_TITLE),
        occurredAt: deployment.deployedAt,
        untrusted: true,
      };
      const url = safeExternalUrl(deployment.url);
      if (url !== undefined) event.url = url;
      return event;
    });
  }

  private githubHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      accept: 'application/vnd.github+json',
      'user-agent': 'kafka-lens',
      'x-github-api-version': '2022-11-28',
    };
    const token = this.options.githubToken;
    if (token !== undefined) headers.authorization = `Bearer ${token}`;
    return headers;
  }

  private async fetchJson(url: URL, headers: Record<string, string>): Promise<unknown> {
    assertSafeExternalUrl(url);
    const fetchImpl = this.options.fetchImpl ?? fetch;
    const response = await fetchImpl(url, {
      headers,
      signal: AbortSignal.timeout(5000),
      redirect: 'manual',
    });
    if (response.status >= 300 && response.status < 400) {
      throw new Error(`redirect blocked (HTTP ${response.status})`);
    }
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const declared = Number(response.headers.get('content-length') ?? '0');
    if (Number.isFinite(declared) && declared > MAX_BODY_BYTES) {
      throw new Error(`correlation payload exceeded ${MAX_BODY_BYTES} bytes`);
    }
    const text = await readBodyCapped(response, MAX_BODY_BYTES);
    return JSON.parse(text) as unknown;
  }

  private safeMessage(err: unknown): string {
    const raw = err instanceof Error ? err.message : 'correlation request failed';
    const secrets = [this.options.githubToken].filter((value): value is string => Boolean(value));
    return redactSecrets(raw, secrets);
  }
}

export function redactSecrets(value: string, secrets: readonly string[]): string {
  let text = value.replace(/Bearer\s+\S+/gi, 'Bearer [redacted]');
  text = text.replace(/\/\/[^/\s@]+@/g, '//');
  for (const secret of secrets) {
    if (secret.length >= 4) text = text.split(secret).join('[redacted]');
  }
  return text;
}

function inWindow(occurredAt: string, window: { since: string; until: string }): boolean {
  const at = Date.parse(occurredAt);
  if (!Number.isFinite(at)) return false;
  return at >= Date.parse(window.since) && at <= Date.parse(window.until) + 1000;
}

/** Reads at most `maxBytes` without buffering an arbitrarily large body. */
async function readBodyCapped(response: Response, maxBytes: number): Promise<string> {
  const body = response.body;
  if (body === null) return '';
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let text = '';
  try {
    for (;;) {
      const next = (await reader.read()) as { done: boolean; value?: Uint8Array };
      if (next.done) break;
      const value = next.value;
      if (value === undefined) continue;
      bytes += value.byteLength;
      if (bytes > maxBytes) {
        await reader.cancel().catch(() => undefined);
        throw new Error(`correlation payload exceeded ${maxBytes} bytes`);
      }
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
    return text;
  } finally {
    reader.releaseLock();
  }
}

function assertSafeExternalUrl(url: URL): void {
  if (url.username !== '' || url.password !== '') {
    throw new Error('correlation URL must not contain credentials');
  }
  if (url.protocol === 'https:') return;
  if (url.protocol === 'http:' && isLoopbackHost(url.hostname)) return;
  throw new Error('correlation URL must use https; http is allowed only for loopback hosts');
}

function isLoopbackHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '');
  return host === 'localhost' || host === '127.0.0.1' || host === '::1';
}

function safeExternalText(value: string, max: number): string {
  return (
    value
      // eslint-disable-next-line no-control-regex -- control and bidi characters are removed on purpose
      .replace(/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, max)
  );
}

function safeExternalUrl(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:' && url.protocol !== 'http:') return undefined;
    return url.toString().slice(0, MAX_URL);
  } catch {
    return undefined;
  }
}
