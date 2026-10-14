import type { Finding } from './findings.js';
import {
  clampConfidence,
  formatMean,
  formatMedian,
  formatRatio,
  isAtLeastRatio,
  parseNonNegativeInteger,
  populationStdDev,
  shareOf,
} from './numbers.js';

const MAX_SHARE_ROWS = 50;
const MAX_OUTLIER_EVIDENCE = 8;

export interface LagShare {
  partitionId: number;
  lag: string;
  share: number;
}

export interface ImbalanceAnalysis {
  insufficientEvidence: boolean;
  knownPartitionCount: number;
  unknownPartitionCount: number;
  totalKnownLag: string | null;
  min: string | null;
  max: string | null;
  mean: string | null;
  median: string | null;
  stdDev: string | null;
  maxMinRatio: string | null;
  outlierPartitionIds: number[];
  shares: LagShare[];
  sharesTruncated: boolean;
  evidence: string[];
  finding: Finding | null;
}

export interface ImbalancePartition {
  partitionId: number;
  lag: string | null;
}

/**
 * Partition imbalance uses PARTITION_IMBALANCE_RATIO.
 *
 * With a positive minimum, a partition is an outlier when its lag is at least
 * `ratio` times that minimum. When the minimum is 0, max/min is undefined, so
 * a partition is an outlier when its lag is at least `ratio` times the mean.
 * Fewer than two known lags is insufficient evidence, not a balanced topic.
 */
export function analyzePartitionImbalance(
  partitions: readonly ImbalancePartition[],
  ratio: number,
): ImbalanceAnalysis {
  const known: { partitionId: number; lag: bigint }[] = [];
  let unknownPartitionCount = 0;
  for (const partition of partitions) {
    const lag = parseNonNegativeInteger(partition.lag);
    if (lag === null) {
      unknownPartitionCount += 1;
    } else {
      known.push({ partitionId: partition.partitionId, lag });
    }
  }

  const empty = emptyAnalysis(known.length, unknownPartitionCount);
  if (known.length < 2) {
    return {
      ...empty,
      insufficientEvidence: true,
      evidence: [
        `Insufficient evidence for partition imbalance: ${known.length} partition(s) have a known lag and ${unknownPartitionCount} are unknown.`,
      ],
    };
  }

  const lags = known.map((partition) => partition.lag);
  const total = lags.reduce((sum, lag) => sum + lag, 0n);
  const min = lags.reduce((current, lag) => (lag < current ? lag : current));
  const max = lags.reduce((current, lag) => (lag > current ? lag : current));
  const mean = formatMean(total, known.length);
  const median = formatMedian(lags);
  const stdDev = populationStdDev(lags);
  const maxMinRatio = formatRatio(max, min);

  const shares: LagShare[] = known
    .map((partition) => ({
      partitionId: partition.partitionId,
      lag: partition.lag.toString(),
      share: shareOf(partition.lag, total),
    }))
    .sort((a, b) => b.share - a.share || a.partitionId - b.partitionId);

  // Strictly above the minimum so a uniform lag is not an outlier when ratio <= 1.
  // When the minimum is 0, compare with the mean because max/min is undefined.
  const outlierPartitionIds = known
    .filter((partition) => {
      if (min > 0n) {
        return partition.lag > min && isAtLeastRatio(partition.lag, min, ratio);
      }
      if (total === 0n) return false;
      const meanLag = total / BigInt(known.length);
      return meanLag > 0n && isAtLeastRatio(partition.lag, meanLag, ratio);
    })
    .map((partition) => partition.partitionId)
    .sort((a, b) => a - b);

  const evidence = [
    `Known lag across ${known.length} partitions: total ${total.toString()}, min ${min.toString()}, max ${max.toString()}, mean ${mean ?? 'unknown'}, median ${median ?? 'unknown'}.`,
  ];
  if (stdDev !== null) {
    evidence.push(`Population standard deviation of known lag is ${stdDev}.`);
  } else {
    evidence.push(
      'Population standard deviation was not computed because a lag exceeds the safe integer range.',
    );
  }
  if (maxMinRatio !== null) {
    evidence.push(
      `Max/min known lag ratio is ${maxMinRatio}. The configured PARTITION_IMBALANCE_RATIO is ${ratio}.`,
    );
  } else {
    evidence.push(
      `Minimum known lag is 0, so max/min is undefined. Outliers are partitions at least ${ratio} times the mean.`,
    );
  }
  const shareById = new Map(shares.map((share) => [share.partitionId, share]));
  for (const partitionId of outlierPartitionIds.slice(0, MAX_OUTLIER_EVIDENCE)) {
    const share = shareById.get(partitionId);
    if (share === undefined) continue;
    evidence.push(
      `Partition ${partitionId} lag is ${share.lag}, which is ${(share.share * 100).toFixed(1)}% of known lag.`,
    );
  }
  if (outlierPartitionIds.length > MAX_OUTLIER_EVIDENCE) {
    evidence.push(
      `${outlierPartitionIds.length - MAX_OUTLIER_EVIDENCE} additional outlier partitions were omitted from the evidence list.`,
    );
  }

  const strongest = shares[0];
  const finding: Finding | null =
    outlierPartitionIds.length === 0
      ? null
      : {
          type: 'PARTITION_IMBALANCE',
          severity: strongest !== undefined && strongest.share >= 0.8 ? 'high' : 'medium',
          confidence: imbalanceConfidence(max, min, total, known.length, ratio),
          evidence: evidence.slice(
            0,
            4 + Math.min(outlierPartitionIds.length, MAX_OUTLIER_EVIDENCE),
          ),
          recommendations: [
            'Compare message keys for the outlier partitions with the others.',
            'Inspect handler time and downstream calls on the outlier partitions.',
            'Confirm the configured PARTITION_IMBALANCE_RATIO matches the skew you care about.',
          ],
        };

  return {
    insufficientEvidence: false,
    knownPartitionCount: known.length,
    unknownPartitionCount,
    totalKnownLag: total.toString(),
    min: min.toString(),
    max: max.toString(),
    mean,
    median,
    stdDev,
    maxMinRatio,
    outlierPartitionIds,
    shares: shares.slice(0, MAX_SHARE_ROWS),
    sharesTruncated: shares.length > MAX_SHARE_ROWS,
    evidence,
    finding,
  };
}

function imbalanceConfidence(
  max: bigint,
  min: bigint,
  total: bigint,
  count: number,
  ratio: number,
): number {
  const baseline = min > 0n ? min : total / BigInt(count);
  if (baseline <= 0n || ratio <= 0) return 0.7;
  const observed = Number(max) / Number(baseline);
  const excess = observed / ratio;
  return clampConfidence(0.65 + Math.min(0.3, (excess - 1) * 0.15));
}

function emptyAnalysis(
  knownPartitionCount: number,
  unknownPartitionCount: number,
): ImbalanceAnalysis {
  return {
    insufficientEvidence: false,
    knownPartitionCount,
    unknownPartitionCount,
    totalKnownLag: null,
    min: null,
    max: null,
    mean: null,
    median: null,
    stdDev: null,
    maxMinRatio: null,
    outlierPartitionIds: [],
    shares: [],
    sharesTruncated: false,
    evidence: [],
    finding: null,
  };
}
