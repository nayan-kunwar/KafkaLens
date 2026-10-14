import type { Finding } from './findings.js';
import { parseNonNegativeInteger } from './numbers.js';

export const PROGRESS_CLASSES = [
  'NOT_ASSIGNED',
  'ASSIGNED_NOT_PROGRESSING',
  'PROGRESSING_SLOWLY',
  'ACTIVELY_PROCESSING',
  'INSUFFICIENT_EVIDENCE',
] as const;

export type ProgressClass = (typeof PROGRESS_CLASSES)[number];

export interface OffsetSample {
  observedAt: string;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
}

export interface StallPartitionInput {
  partitionId: number;
  /** Null means the assignment could not be decoded. False means no member has the partition. */
  assigned: boolean | null;
  lag: string | null;
  samples: readonly OffsetSample[];
}

export interface PartitionProgress {
  partitionId: number;
  progress: ProgressClass;
  evidence: string[];
}

export interface StallAnalysis {
  windowSeconds: number;
  partitions: PartitionProgress[];
  groupProgress: ProgressClass;
  finding: Finding | null;
  slowFinding: Finding | null;
}

const COVERAGE_FRACTION = 0.8;

/**
 * Classifies consumer progress inside STALL_WINDOW_SECONDS.
 *
 * An assigned consumer whose committed offset did not advance is
 * ASSIGNED_NOT_PROGRESSING. That is not a crash: the process can be blocked,
 * paused, or waiting on a dependency. Missing history stays
 * INSUFFICIENT_EVIDENCE instead of a stall.
 */
export function analyzeConsumerProgress(
  partitions: readonly StallPartitionInput[],
  windowSeconds: number,
  now: Date,
): StallAnalysis {
  const rows = partitions.map((partition) =>
    classifyPartition(partition, windowSeconds, now.getTime()),
  );
  const stalled = rows.filter((row) => row.progress === 'ASSIGNED_NOT_PROGRESSING');
  const slow = rows.filter((row) => row.progress === 'PROGRESSING_SLOWLY');

  return {
    windowSeconds,
    partitions: rows,
    groupProgress: summarize(rows.map((row) => row.progress)),
    finding: stalled.length === 0 ? null : stallFinding(stalled, windowSeconds),
    slowFinding: slow.length === 0 ? null : slowFinding(slow, windowSeconds),
  };
}

function classifyPartition(
  partition: StallPartitionInput,
  windowSeconds: number,
  nowMs: number,
): PartitionProgress {
  if (partition.assigned === false) {
    return {
      partitionId: partition.partitionId,
      progress: 'NOT_ASSIGNED',
      evidence: [`Partition ${partition.partitionId} is not assigned to a member.`],
    };
  }
  if (partition.assigned === null) {
    return {
      partitionId: partition.partitionId,
      progress: 'INSUFFICIENT_EVIDENCE',
      evidence: [
        `Assignment for partition ${partition.partitionId} is unknown, so progress was not classified.`,
      ],
    };
  }

  const windowStart = nowMs - windowSeconds * 1000;
  const samples = partition.samples
    .map((sample) => ({ sample, at: Date.parse(sample.observedAt) }))
    .filter(
      (entry) => Number.isFinite(entry.at) && entry.at >= windowStart && entry.at <= nowMs + 1000,
    )
    .sort((a, b) => a.at - b.at);

  const committed = samples.filter(
    (entry) => parseNonNegativeInteger(entry.sample.committedOffset) !== null,
  );
  const first = committed[0];
  const last = committed[committed.length - 1];
  if (first === undefined || last === undefined || first === last) {
    return {
      partitionId: partition.partitionId,
      progress: 'INSUFFICIENT_EVIDENCE',
      evidence: [
        `Partition ${partition.partitionId} has ${committed.length} committed-offset sample(s) inside ${windowSeconds}s. At least two samples covering ${(COVERAGE_FRACTION * 100).toFixed(0)}% of the window are required.`,
      ],
    };
  }

  const spanMs = last.at - first.at;
  const requiredMs = windowSeconds * 1000 * COVERAGE_FRACTION;
  if (spanMs < requiredMs) {
    return {
      partitionId: partition.partitionId,
      progress: 'INSUFFICIENT_EVIDENCE',
      evidence: [
        `Partition ${partition.partitionId} samples span ${Math.round(spanMs / 1000)}s, which is below ${(COVERAGE_FRACTION * 100).toFixed(0)}% of the ${windowSeconds}s stall window.`,
      ],
    };
  }

  const startOffset = parseNonNegativeInteger(first.sample.committedOffset);
  const endOffset = parseNonNegativeInteger(last.sample.committedOffset);
  if (startOffset === null || endOffset === null) {
    return {
      partitionId: partition.partitionId,
      progress: 'INSUFFICIENT_EVIDENCE',
      evidence: [`Partition ${partition.partitionId} committed offsets could not be parsed.`],
    };
  }

  const range = `${first.sample.observedAt} to ${last.sample.observedAt}`;
  if (endOffset < startOffset) {
    return {
      partitionId: partition.partitionId,
      progress: 'INSUFFICIENT_EVIDENCE',
      evidence: [
        `Partition ${partition.partitionId} committed offset decreased from ${startOffset.toString()} to ${endOffset.toString()} between ${range}. That can follow an offset change outside this server. Progress was not classified as a stall.`,
      ],
    };
  }

  if (endOffset === startOffset) {
    if (partition.lag === null) {
      return {
        partitionId: partition.partitionId,
        progress: 'INSUFFICIENT_EVIDENCE',
        evidence: [
          `Partition ${partition.partitionId} committed offset stayed at ${endOffset.toString()} between ${range}, and current lag is unknown.`,
        ],
      };
    }
    if (partition.lag === '0') {
      return {
        partitionId: partition.partitionId,
        progress: 'ACTIVELY_PROCESSING',
        evidence: [
          `Partition ${partition.partitionId} is caught up (lag 0) and the committed offset stayed at ${endOffset.toString()} between ${range}.`,
        ],
      };
    }
    return {
      partitionId: partition.partitionId,
      progress: 'ASSIGNED_NOT_PROGRESSING',
      evidence: [
        `Partition ${partition.partitionId} is assigned and its committed offset stayed at ${endOffset.toString()} between ${range} while lag was ${partition.lag}. This shows no progress. It does not show that the process crashed.`,
      ],
    };
  }

  const startLag = parseNonNegativeInteger(first.sample.lag);
  const endLag = parseNonNegativeInteger(partition.lag ?? last.sample.lag);
  if (startLag !== null && endLag !== null && endLag > startLag) {
    return {
      partitionId: partition.partitionId,
      progress: 'PROGRESSING_SLOWLY',
      evidence: [
        `Partition ${partition.partitionId} committed offset advanced from ${startOffset.toString()} to ${endOffset.toString()} between ${range}, while lag grew from ${startLag.toString()} to ${endLag.toString()}.`,
      ],
    };
  }

  return {
    partitionId: partition.partitionId,
    progress: 'ACTIVELY_PROCESSING',
    evidence: [
      `Partition ${partition.partitionId} committed offset advanced from ${startOffset.toString()} to ${endOffset.toString()} between ${range}.`,
    ],
  };
}

function summarize(classes: readonly ProgressClass[]): ProgressClass {
  if (classes.length === 0) return 'INSUFFICIENT_EVIDENCE';
  if (classes.some((progress) => progress === 'ASSIGNED_NOT_PROGRESSING')) {
    return 'ASSIGNED_NOT_PROGRESSING';
  }
  if (classes.some((progress) => progress === 'PROGRESSING_SLOWLY')) return 'PROGRESSING_SLOWLY';
  if (classes.every((progress) => progress === 'NOT_ASSIGNED')) return 'NOT_ASSIGNED';
  if (classes.some((progress) => progress === 'ACTIVELY_PROCESSING')) return 'ACTIVELY_PROCESSING';
  return 'INSUFFICIENT_EVIDENCE';
}

function stallFinding(rows: readonly PartitionProgress[], windowSeconds: number): Finding {
  return {
    type: 'CONSUMER_STALLED',
    severity: 'high',
    confidence: 0.84,
    evidence: rows.slice(0, 5).flatMap((row) => row.evidence),
    recommendations: [
      `Check the member assigned to the stalled partition for a blocked handler or a paused consumer over the ${windowSeconds}s window.`,
      'Do not treat a lack of offset progress as proof that the process has crashed.',
      'Re-run the diagnosis after the stall window if this sample does not cover it.',
    ],
  };
}

function slowFinding(rows: readonly PartitionProgress[], windowSeconds: number): Finding {
  return {
    type: 'CONSUMER_SLOW',
    severity: 'medium',
    confidence: 0.74,
    evidence: rows.slice(0, 5).flatMap((row) => row.evidence),
    recommendations: [
      `Profile processing time for the slow partitions across the ${windowSeconds}s window.`,
      'Compare estimated incoming and processing rates before adding consumers.',
    ],
  };
}
