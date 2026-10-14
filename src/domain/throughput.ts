import type { Finding } from './findings.js';
import { clampConfidence, parseNonNegativeInteger, roundRate } from './numbers.js';
import type { OffsetSample } from './stall.js';

export interface RateEstimate {
  messagesPerSecond: number | null;
  sampleCount: number;
  windowSeconds: number | null;
  excludedPartitionCount: number;
  /** Rates are offset deltas over the stored window, not broker producer metrics. */
  label: 'estimate';
  insufficientEvidence: boolean;
}

export interface ThroughputAnalysis {
  incoming: RateEstimate;
  processing: RateEstimate;
  lagGrowth: RateEstimate;
  /** Null when either rate, or lag growth, cannot be estimated. */
  producerOutpacing: boolean | null;
  evidence: string[];
  finding: Finding | null;
}

export interface ThroughputPartitionInput {
  partitionId: number;
  samples: readonly OffsetSample[];
}

const EMPTY_RATE: RateEstimate = {
  messagesPerSecond: null,
  sampleCount: 0,
  windowSeconds: null,
  excludedPartitionCount: 0,
  label: 'estimate',
  insufficientEvidence: true,
};

/**
 * Estimates incoming rate from high-watermark movement, processing rate from
 * committed-offset movement, and lag growth from lag movement.
 * A negative delta is excluded rather than clamped to zero.
 */
export function analyzeThroughput(
  partitions: readonly ThroughputPartitionInput[],
  now: Date,
  windowSeconds: number,
): ThroughputAnalysis {
  const windowStart = now.getTime() - windowSeconds * 1000;
  let incoming = 0;
  let processing = 0;
  let lagGrowth = 0;
  let incomingCount = 0;
  let processingCount = 0;
  let lagCount = 0;
  let excluded = 0;
  let spanSeconds = 0;
  let sampleCount = 0;

  for (const partition of partitions) {
    const samples = partition.samples
      .map((sample) => ({ sample, at: Date.parse(sample.observedAt) }))
      .filter(
        (entry) =>
          Number.isFinite(entry.at) && entry.at >= windowStart && entry.at <= now.getTime() + 1000,
      )
      .sort((a, b) => a.at - b.at);
    sampleCount += samples.length;
    const first = samples[0];
    const last = samples[samples.length - 1];
    if (first === undefined || last === undefined || first === last) {
      excluded += 1;
      continue;
    }
    const seconds = (last.at - first.at) / 1000;
    if (seconds <= 0) {
      excluded += 1;
      continue;
    }
    spanSeconds = Math.max(spanSeconds, seconds);
    const incomingRate = deltaRate(first.sample.highWatermark, last.sample.highWatermark, seconds);
    const processingRate = deltaRate(
      first.sample.committedOffset,
      last.sample.committedOffset,
      seconds,
    );
    const growthRate = deltaRate(first.sample.lag, last.sample.lag, seconds, true);
    if (incomingRate === null && processingRate === null && growthRate === null) {
      excluded += 1;
      continue;
    }
    if (incomingRate !== null) {
      incoming += incomingRate;
      incomingCount += 1;
    }
    if (processingRate !== null) {
      processing += processingRate;
      processingCount += 1;
    }
    if (growthRate !== null) {
      lagGrowth += growthRate;
      lagCount += 1;
    }
  }

  const incomingEstimate = rate(incoming, incomingCount, sampleCount, spanSeconds, excluded);
  const processingEstimate = rate(processing, processingCount, sampleCount, spanSeconds, excluded);
  const lagEstimate = rate(lagGrowth, lagCount, sampleCount, spanSeconds, excluded);
  const comparable =
    incomingEstimate.messagesPerSecond !== null &&
    processingEstimate.messagesPerSecond !== null &&
    lagEstimate.messagesPerSecond !== null;
  const producerOutpacing = comparable
    ? greater(
        incomingEstimate.messagesPerSecond as number,
        processingEstimate.messagesPerSecond as number,
      ) && (lagEstimate.messagesPerSecond as number) > 0
    : null;

  const evidence = [
    describeRate('Estimated incoming rate', incomingEstimate),
    describeRate('Estimated processing rate', processingEstimate),
    describeRate('Estimated lag growth', lagEstimate),
    'These rates are estimates from stored high watermarks and committed offsets. They are not broker producer metrics.',
  ];
  if (producerOutpacing === true) {
    evidence.push(
      'Estimated incoming rate exceeded estimated processing rate while lag was growing. That is a possible reason for the increase.',
    );
  } else if (producerOutpacing === false) {
    evidence.push(
      'The estimates do not show incoming rate above processing rate with growing lag.',
    );
  } else {
    evidence.push('There is not enough history to compare incoming and processing rates.');
  }

  const finding: Finding | null = producerOutpacing
    ? {
        type: 'PRODUCER_OUTPACING_CONSUMER',
        severity:
          incomingEstimate.messagesPerSecond !== null &&
          processingEstimate.messagesPerSecond !== null &&
          incomingEstimate.messagesPerSecond >= processingEstimate.messagesPerSecond * 2
            ? 'high'
            : 'medium',
        confidence: clampConfidence(0.72 + Math.min(0.15, spanSeconds / 1000)),
        evidence,
        recommendations: [
          'Treat the rates as estimates and confirm them with producer metrics if you have them.',
          'Inspect consumer handler time before changing partition count or producer traffic.',
        ],
      }
    : null;

  return {
    incoming: incomingEstimate,
    processing: processingEstimate,
    lagGrowth: lagEstimate,
    producerOutpacing,
    evidence,
    finding,
  };
}

function deltaRate(
  start: string | null,
  end: string | null,
  seconds: number,
  allowDecrease = false,
): number | null {
  const from = parseNonNegativeInteger(start);
  const to = parseNonNegativeInteger(end);
  if (from === null || to === null || seconds <= 0) return null;
  if (to < from && !allowDecrease) return null;
  const delta = to - from;
  const magnitude = delta < 0n ? -delta : delta;
  if (magnitude > BigInt(Number.MAX_SAFE_INTEGER)) return null;
  return Number(delta) / seconds;
}

function greater(left: number, right: number): boolean {
  return left > right + Math.max(0.01, Math.abs(right) * 0.01);
}

function rate(
  sum: number,
  contributing: number,
  sampleCount: number,
  spanSeconds: number,
  excluded: number,
): RateEstimate {
  if (contributing === 0) {
    return { ...EMPTY_RATE, sampleCount, excludedPartitionCount: excluded };
  }
  return {
    messagesPerSecond: roundRate(sum),
    sampleCount,
    windowSeconds: roundRate(spanSeconds),
    excludedPartitionCount: excluded,
    label: 'estimate',
    insufficientEvidence: false,
  };
}

function describeRate(label: string, estimate: RateEstimate): string {
  if (estimate.messagesPerSecond === null || estimate.windowSeconds === null) {
    return `${label}: insufficient evidence (${estimate.sampleCount} samples, ${estimate.excludedPartitionCount} partitions excluded).`;
  }
  return `${label}: ${estimate.messagesPerSecond} messages/second over about ${estimate.windowSeconds}s (${estimate.excludedPartitionCount} partitions excluded).`;
}
