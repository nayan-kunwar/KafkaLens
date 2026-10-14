import type { Logger } from 'pino';
import { z } from 'zod';

export interface CorrelationEvent {
  source: 'github' | 'deployment';
  id: string;
  title: string;
  occurredAt: string;
  url?: string;
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
  logger?: Logger | undefined;
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
const MAX_BODY = 1_000_000;

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
        id: commit.sha.slice(0, 12),
        title: title.slice(0, 200),
        occurredAt: commit.commit.author?.date ?? window.until,
      };
      if (commit.html_url !== undefined) event.url = commit.html_url;
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
        title: title.slice(0, 200),
        occurredAt: deployment.created_at,
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
        id: String(deployment.id),
        title: `${deployment.service} ${deployment.version}`.slice(0, 200),
        occurredAt: deployment.deployedAt,
      };
      if (deployment.url !== undefined) event.url = deployment.url;
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
    const fetchImpl = this.options.fetchImpl ?? fetch;
    const response = await fetchImpl(url, { headers, signal: AbortSignal.timeout(5000) });
    const text = await response.text();
    if (text.length > MAX_BODY) {
      throw new Error('correlation payload exceeded 1000000 bytes');
    }
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
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
