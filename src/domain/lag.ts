export type LagUnknownReason = 'NO_COMMITTED_OFFSET' | 'UNKNOWN_WATERMARK' | 'INVALID_OFFSET';

export interface PartitionLag {
  lag: string | null;
  unknownReason?: LagUnknownReason;
  adjusted?: true;
}

export interface LagTotal {
  total: string | null;
  unknownCount: number;
}

const DECIMAL_STRING = /^\d+$/;

/**
 * Computes lag for one partition as `highWatermark - committedOffset`.
 *
 * Unknown is never silently converted to zero: a missing high watermark, a
 * missing committed offset, or the Kafka `-1` no-commit sentinel all yield
 * `lag: null` with a reason. A negative difference (transient, e.g. after
 * retention) is clamped to `'0'` and flagged with `adjusted: true`.
 */
export function calculatePartitionLag(
  highWatermark: string | null | undefined,
  committedOffset: string | null | undefined,
): PartitionLag {
  if (highWatermark == null) {
    return { lag: null, unknownReason: 'UNKNOWN_WATERMARK' };
  }
  if (committedOffset == null || committedOffset === '-1') {
    return { lag: null, unknownReason: 'NO_COMMITTED_OFFSET' };
  }
  if (!DECIMAL_STRING.test(highWatermark) || !DECIMAL_STRING.test(committedOffset)) {
    return { lag: null, unknownReason: 'INVALID_OFFSET' };
  }
  const diff = BigInt(highWatermark) - BigInt(committedOffset);
  if (diff < 0n) {
    return { lag: '0', adjusted: true };
  }
  return { lag: diff.toString() };
}

/**
 * Sums known partition lags. Unknown partitions are counted, never treated
 * as zero: `total` is `null` when every input is unknown.
 */
export function sumLags(lags: readonly (string | null)[]): LagTotal {
  let total = 0n;
  let unknownCount = 0;
  let knownCount = 0;
  for (const lag of lags) {
    if (lag == null) {
      unknownCount += 1;
    } else {
      total += BigInt(lag);
      knownCount += 1;
    }
  }
  return { total: knownCount === 0 ? null : total.toString(), unknownCount };
}
