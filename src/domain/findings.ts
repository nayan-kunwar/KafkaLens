export const FINDING_TYPES = [
  'CONSUMER_STALLED',
  'CONSUMER_SLOW',
  'PARTITION_IMBALANCE',
  'INSUFFICIENT_CONSUMER_CAPACITY',
  'PRODUCER_OUTPACING_CONSUMER',
  'REBALANCING',
  'NO_COMMITTED_OFFSET',
  'UNKNOWN',
] as const;

export type FindingType = (typeof FINDING_TYPES)[number];

export const SEVERITIES = ['low', 'medium', 'high', 'critical'] as const;

export type Severity = (typeof SEVERITIES)[number];

export interface Finding {
  type: FindingType;
  severity: Severity;
  confidence: number;
  evidence: string[];
  recommendations: string[];
}

export type StatementKind = 'observed_fact' | 'derived_metric' | 'inference' | 'recommendation';

export interface Statement {
  kind: StatementKind;
  text: string;
}

const SEVERITY_RANK: Record<Severity, number> = {
  critical: 4,
  high: 3,
  medium: 2,
  low: 1,
};

export function rankFindings(findings: readonly Finding[]): Finding[] {
  return [...findings].sort((a, b) => {
    const bySeverity = SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity];
    if (bySeverity !== 0) return bySeverity;
    if (a.confidence !== b.confidence) return b.confidence - a.confidence;
    return a.type.localeCompare(b.type);
  });
}

export function highestSeverity(findings: readonly Finding[]): Severity | null {
  const ranked = rankFindings(findings);
  return ranked[0]?.severity ?? null;
}
