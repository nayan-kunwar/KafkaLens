import {
  highestSeverity,
  rankFindings,
  type Finding,
  type Severity,
  type Statement,
} from './findings.js';
import { analyzePartitionImbalance, type ImbalanceAnalysis } from './imbalance.js';
import { sumLags, type LagUnknownReason } from './lag.js';
import { parseNonNegativeInteger } from './numbers.js';
import { analyzeConsumerProgress, type OffsetSample, type StallAnalysis } from './stall.js';
import { analyzeThroughput, type ThroughputAnalysis } from './throughput.js';

export type OverallStatus = 'no_issue_detected' | 'attention' | 'insufficient_evidence';

export interface DiagnosisPartition {
  partitionId: number;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
  unknownReason?: LagUnknownReason;
  /** Null when assignments could not be decoded. */
  assigned: boolean | null;
  samples: readonly OffsetSample[];
}

export interface DiagnosisSnapshot {
  groupId: string;
  topic: string;
  groupState: string;
  memberCount: number;
  partitions: readonly DiagnosisPartition[];
  historyAvailable: boolean;
  imbalanceRatio: number;
  stallWindowSeconds: number;
  now: Date;
  coverageIncomplete: boolean;
}

export interface DiagnosisImpact {
  totalLag: string | null;
  unknownPartitionCount: number;
  laggingPartitionCount: number;
  highestSeverity: Severity | null;
  summary: string;
}

export interface DiagnosisTimeline {
  windowSeconds: number;
  sampleCount: number;
  earliestObservedAt: string | null;
  latestObservedAt: string | null;
  historyAvailable: boolean;
}

export interface DiagnosisResult {
  status: OverallStatus;
  findings: Finding[];
  evidence: Statement[];
  likelyCauses: string[];
  confidence: number | null;
  recommendedSteps: string[];
  impact: DiagnosisImpact;
  timeline: DiagnosisTimeline;
  imbalance: ImbalanceAnalysis;
  stall: StallAnalysis;
  throughput: ThroughputAnalysis;
}

const REBALANCE_STATES = new Set(['PreparingRebalance', 'CompletingRebalance']);
const MAX_STEPS = 12;

export function diagnose(snapshot: DiagnosisSnapshot): DiagnosisResult {
  const imbalance = analyzePartitionImbalance(
    snapshot.partitions.map((partition) => ({
      partitionId: partition.partitionId,
      lag: partition.lag,
    })),
    snapshot.imbalanceRatio,
  );
  const stall = analyzeConsumerProgress(
    snapshot.partitions.map((partition) => ({
      partitionId: partition.partitionId,
      assigned: partition.assigned,
      lag: partition.lag,
      samples: partition.samples,
    })),
    snapshot.stallWindowSeconds,
    snapshot.now,
  );
  const throughput = analyzeThroughput(
    snapshot.partitions.map((partition) => ({
      partitionId: partition.partitionId,
      samples: partition.samples,
    })),
    snapshot.now,
    snapshot.stallWindowSeconds,
  );

  const totals = sumLags(snapshot.partitions.map((partition) => partition.lag));
  const laggingPartitionCount = snapshot.partitions.filter((partition) => {
    const lag = parseNonNegativeInteger(partition.lag);
    return lag !== null && lag > 0n;
  }).length;
  const rebalancing = REBALANCE_STATES.has(snapshot.groupState);
  const findings: Finding[] = [];

  if (rebalancing) {
    findings.push(rebalanceFinding(snapshot.groupState));
  }
  const noCommit = noCommitFinding(snapshot.partitions);
  if (noCommit !== null) findings.push(noCommit);
  const capacity = capacityFinding(snapshot, totals.total, throughput.producerOutpacing);
  if (capacity !== null) findings.push(capacity);
  if (imbalance.finding !== null) findings.push(imbalance.finding);
  if (!rebalancing && stall.finding !== null) findings.push(stall.finding);
  if (!rebalancing && stall.slowFinding !== null) findings.push(stall.slowFinding);
  if (throughput.finding !== null) findings.push(throughput.finding);

  const positiveLag =
    totals.total !== null &&
    parseNonNegativeInteger(totals.total) !== null &&
    BigInt(totals.total) > 0n;
  if (findings.length === 0 && (positiveLag || hasUnclassifiedData(snapshot))) {
    findings.push(unknownFinding(snapshot, totals.total));
  }

  const ranked = rankFindings(findings).map((finding) =>
    snapshot.coverageIncomplete ? dampen(finding) : finding,
  );
  const evidence = buildEvidence(
    snapshot,
    imbalance,
    stall,
    throughput,
    totals,
    ranked,
    rebalancing,
  );
  const likelyCauses = ranked.map(likelyCause);
  const recommendedSteps = unique(ranked.flatMap((finding) => finding.recommendations)).slice(
    0,
    MAX_STEPS,
  );

  return {
    status: overallStatus(ranked, positiveLag),
    findings: ranked,
    evidence,
    likelyCauses,
    confidence: ranked[0]?.confidence ?? null,
    recommendedSteps,
    impact: {
      totalLag: totals.total,
      unknownPartitionCount: totals.unknownCount,
      laggingPartitionCount,
      highestSeverity: highestSeverity(ranked),
      summary: impactSummary(totals.total, totals.unknownCount, laggingPartitionCount, ranked),
    },
    timeline: timeline(snapshot),
    imbalance,
    stall,
    throughput,
  };
}

function overallStatus(findings: readonly Finding[], positiveLag: boolean): OverallStatus {
  if (findings.length === 0) return 'no_issue_detected';
  if (findings.every((finding) => finding.type === 'UNKNOWN') && !positiveLag) {
    return 'insufficient_evidence';
  }
  if (findings.every((finding) => finding.type === 'UNKNOWN')) return 'insufficient_evidence';
  return 'attention';
}

function rebalanceFinding(state: string): Finding {
  return {
    type: 'REBALANCING',
    severity: 'medium',
    confidence: 0.92,
    evidence: [
      `Consumer group state is ${state}. Members can pause progress while a rebalance is in progress.`,
    ],
    recommendations: [
      'Read the group again after it returns to Stable before treating idle offsets as a stall.',
      'Look for members joining or leaving if the group stays in a rebalance state.',
    ],
  };
}

function noCommitFinding(partitions: readonly DiagnosisPartition[]): Finding | null {
  const missing = partitions.filter(
    (partition) =>
      partition.unknownReason === 'NO_COMMITTED_OFFSET' &&
      watermarkHasData(partition.highWatermark),
  );
  if (missing.length === 0) return null;
  const ids = missing
    .slice(0, 8)
    .map((partition) => partition.partitionId)
    .join(', ');
  return {
    type: 'NO_COMMITTED_OFFSET',
    severity: missing.length === partitions.length ? 'high' : 'medium',
    confidence: 0.9,
    evidence: [
      `${missing.length} partition(s) have a high watermark above 0 and no committed offset (${ids}). Unknown lag was not treated as zero.`,
    ],
    recommendations: [
      'Confirm this group is the one expected to commit offsets for the topic.',
      'A new group stays without commits until the first successful commit.',
    ],
  };
}

function capacityFinding(
  snapshot: DiagnosisSnapshot,
  totalLag: string | null,
  producerOutpacing: boolean | null,
): Finding | null {
  const lag = parseNonNegativeInteger(totalLag);
  const hasLag = lag !== null && lag > 0n;
  if (!hasLag) return null;
  if (snapshot.memberCount === 0) {
    return {
      type: 'INSUFFICIENT_CONSUMER_CAPACITY',
      severity: 'high',
      confidence: 0.86,
      evidence: [
        `Consumer group ${snapshot.groupId} has 0 members while known lag is ${totalLag}. No member is assigned. This does not by itself show that a process crashed.`,
      ],
      recommendations: [
        'Check whether a consumer using this group id is running and able to join.',
        'Confirm the group id and topic match the deployment that should be reading.',
      ],
    };
  }
  const partitionCount = snapshot.partitions.length;
  if (
    producerOutpacing === true &&
    snapshot.memberCount < partitionCount &&
    snapshot.groupState === 'Stable'
  ) {
    return {
      type: 'INSUFFICIENT_CONSUMER_CAPACITY',
      severity: 'medium',
      confidence: 0.58,
      evidence: [
        `Possible capacity limit: ${snapshot.memberCount} member(s) for ${partitionCount} partition(s) while estimated incoming rate exceeds estimated processing rate and lag is growing. One member can read every partition, so this is a hypothesis, not a measurement of saturation.`,
      ],
      recommendations: [
        'Compare processing rate with the current membership before adding consumers.',
        'Check per-partition handler time. Extra members only help when work is spread across partitions.',
      ],
    };
  }
  return null;
}

function unknownFinding(snapshot: DiagnosisSnapshot, totalLag: string | null): Finding {
  const history = snapshot.historyAvailable
    ? 'Stored history does not identify a stall, skew, or rate gap.'
    : 'Lag history is unavailable, so stall detection and throughput estimates cannot be completed.';
  return {
    type: 'UNKNOWN',
    severity: 'low',
    confidence: 0.4,
    evidence: [
      `Insufficient evidence to classify group ${snapshot.groupId} on ${snapshot.topic}. Known total lag is ${totalLag ?? 'unknown'}. ${history}`,
    ],
    recommendations: [
      'Collect at least one more lag sample that covers STALL_WINDOW_SECONDS, then diagnose again.',
      'Do not pick a cause until a finding cites offsets, rates, or membership.',
    ],
  };
}

function hasUnclassifiedData(snapshot: DiagnosisSnapshot): boolean {
  return snapshot.partitions.some(
    (partition) =>
      partition.unknownReason === 'INVALID_OFFSET' ||
      (partition.unknownReason === 'UNKNOWN_WATERMARK' && snapshot.memberCount > 0),
  );
}

function watermarkHasData(highWatermark: string | null): boolean {
  const value = parseNonNegativeInteger(highWatermark);
  return value !== null && value > 0n;
}

function dampen(finding: Finding): Finding {
  return {
    ...finding,
    confidence: Math.round(finding.confidence * 0.8 * 100) / 100,
    evidence: [
      ...finding.evidence,
      'Confidence is reduced because the partition page or member list was truncated.',
    ],
  };
}

function buildEvidence(
  snapshot: DiagnosisSnapshot,
  imbalance: ImbalanceAnalysis,
  stall: StallAnalysis,
  throughput: ThroughputAnalysis,
  totals: { total: string | null; unknownCount: number },
  findings: readonly Finding[],
  rebalancing: boolean,
): Statement[] {
  const statements: Statement[] = [
    {
      kind: 'observed_fact',
      text: `Consumer group ${snapshot.groupId} on ${snapshot.topic} is ${snapshot.groupState} with ${snapshot.memberCount} member(s) and ${snapshot.partitions.length} partition(s) in this response.`,
    },
    {
      kind: 'observed_fact',
      text: `Known total lag is ${totals.total ?? 'unknown'}. ${totals.unknownCount} partition(s) have unknown lag.`,
    },
  ];
  if (!snapshot.historyAvailable) {
    statements.push({
      kind: 'observed_fact',
      text: 'Lag history could not be read. Progress and rate findings that need a window were not invented.',
    });
  }
  if (snapshot.coverageIncomplete) {
    statements.push({
      kind: 'observed_fact',
      text: 'The partition or member page was truncated. Statistics describe the returned page.',
    });
  }
  for (const line of imbalance.evidence) {
    statements.push({ kind: 'derived_metric', text: line });
  }
  for (const line of throughput.evidence) {
    statements.push({ kind: 'derived_metric', text: line });
  }
  for (const partition of stall.partitions.slice(0, 8)) {
    for (const line of partition.evidence) {
      statements.push({ kind: 'observed_fact', text: line });
    }
  }
  if (rebalancing) {
    statements.push({
      kind: 'inference',
      text: 'Stall and slowness findings are withheld while the group is rebalancing, because offset progress can pause for that reason.',
    });
  }
  for (const finding of findings) {
    statements.push({ kind: 'inference', text: likelyCause(finding) });
  }
  for (const step of unique(findings.flatMap((finding) => finding.recommendations)).slice(
    0,
    MAX_STEPS,
  )) {
    statements.push({ kind: 'recommendation', text: step });
  }
  return statements.slice(0, 60);
}

function likelyCause(finding: Finding): string {
  const lead: Record<Finding['type'], string> = {
    CONSUMER_STALLED:
      'Evidence suggests an assigned consumer made no offset progress while lag was above zero. That is not evidence of a crash.',
    CONSUMER_SLOW:
      'Evidence suggests the consumer is committing, and lag on at least one partition still grew.',
    PARTITION_IMBALANCE:
      'Evidence suggests known lag is concentrated relative to PARTITION_IMBALANCE_RATIO.',
    INSUFFICIENT_CONSUMER_CAPACITY:
      'Evidence suggests the group may not have enough active members for the current lag. This is a capacity hypothesis.',
    PRODUCER_OUTPACING_CONSUMER:
      'Evidence suggests the estimated incoming rate is above the estimated processing rate while lag grows.',
    REBALANCING:
      'The group is rebalancing. Progress during this state is not a reliable stall signal.',
    NO_COMMITTED_OFFSET:
      'At least one partition has messages and no committed offset, so its lag is unknown rather than zero.',
    UNKNOWN: 'The available observations do not justify a more specific classification.',
  };
  return `${finding.type} (${finding.severity}, confidence ${finding.confidence.toFixed(2)}): ${lead[finding.type]}`;
}

function impactSummary(
  totalLag: string | null,
  unknownCount: number,
  laggingPartitionCount: number,
  findings: readonly Finding[],
): string {
  const top = findings[0];
  const classification =
    top === undefined ? 'No finding was raised.' : `Top finding is ${top.type}.`;
  return `Known total lag is ${totalLag ?? 'unknown'} across ${laggingPartitionCount} lagging partition(s), with ${unknownCount} unknown partition(s). ${classification}`;
}

function timeline(snapshot: DiagnosisSnapshot): DiagnosisTimeline {
  const stamps = snapshot.partitions
    .flatMap((partition) => partition.samples.map((sample) => sample.observedAt))
    .filter((stamp) => Number.isFinite(Date.parse(stamp)))
    .sort();
  return {
    windowSeconds: snapshot.stallWindowSeconds,
    sampleCount: snapshot.partitions.reduce((sum, partition) => sum + partition.samples.length, 0),
    earliestObservedAt: stamps[0] ?? null,
    latestObservedAt: stamps[stamps.length - 1] ?? null,
    historyAvailable: snapshot.historyAvailable,
  };
}

function unique(values: readonly string[]): string[] {
  return [...new Set(values)];
}
