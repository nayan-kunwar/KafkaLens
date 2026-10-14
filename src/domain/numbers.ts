const SCALE = 100n;

export function parseNonNegativeInteger(value: string | null | undefined): bigint | null {
  if (value == null || !/^\d+$/.test(value)) return null;
  return BigInt(value);
}

export function formatMean(sum: bigint, count: number): string | null {
  if (count <= 0) return null;
  return formatScaled((sum * SCALE) / BigInt(count));
}

export function formatRatio(numerator: bigint, denominator: bigint): string | null {
  if (denominator <= 0n) return null;
  return formatScaled((numerator * SCALE) / denominator);
}

/** Median of non-negative integers. An even count uses the mean of the two central values. */
export function formatMedian(values: readonly bigint[]): string | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    const center = sorted[mid];
    return center === undefined ? null : center.toString();
  }
  const left = sorted[mid - 1];
  const right = sorted[mid];
  if (left === undefined || right === undefined) return null;
  return formatScaled(((left + right) * SCALE) / 2n);
}

export function populationStdDev(values: readonly bigint[]): string | null {
  if (values.length === 0) return null;
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
  if (values.some((value) => value > maxSafe)) return null;
  const nums = values.map((value) => Number(value));
  const mean = nums.reduce((sum, value) => sum + value, 0) / nums.length;
  const variance = nums.reduce((sum, value) => sum + (value - mean) ** 2, 0) / nums.length;
  return (Math.round(Math.sqrt(variance) * 100) / 100).toString();
}

/** True when `value / baseline >= ratio`. Baseline must be positive. */
export function isAtLeastRatio(value: bigint, baseline: bigint, ratio: number): boolean {
  if (baseline <= 0n || ratio <= 0) return false;
  const ratioScaled = BigInt(Math.round(ratio * 10_000));
  return value * 10_000n >= baseline * ratioScaled;
}

export function shareOf(part: bigint, total: bigint): number {
  if (total <= 0n) return 0;
  return Number((part * 10_000n) / total) / 10_000;
}

export function roundRate(value: number): number {
  return Math.round(value * 1000) / 1000;
}

export function clampConfidence(value: number): number {
  const clamped = Math.min(0.95, Math.max(0.05, value));
  return Math.round(clamped * 100) / 100;
}

function formatScaled(scaledBy100: bigint): string {
  const negative = scaledBy100 < 0n;
  const absolute = negative ? -scaledBy100 : scaledBy100;
  const whole = absolute / SCALE;
  const frac = (absolute % SCALE).toString().padStart(2, '0');
  const body = frac === '00' ? whole.toString() : `${whole.toString()}.${frac}`;
  return negative ? `-${body}` : body;
}
