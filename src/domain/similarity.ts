export interface SimilarityQuery {
  groupId: string;
  topic: string;
  findingTypes: readonly string[];
}

export interface SimilarityCandidate {
  consumerGroup: string;
  topic: string;
  findingTypes: readonly string[];
}

/**
 * Deterministic similarity in [0, 1].
 * Same group is 0.45, same topic is 0.35, and finding-type Jaccard is 0.20.
 * There is no embedding model. A score of 0 means the rows do not overlap.
 */
export function scoreIncident(query: SimilarityQuery, candidate: SimilarityCandidate): number {
  let score = 0;
  if (candidate.consumerGroup === query.groupId) score += 0.45;
  if (candidate.topic === query.topic) score += 0.35;
  score += 0.2 * jaccard(query.findingTypes, candidate.findingTypes);
  return Math.round(score * 100) / 100;
}

export function jaccard(left: readonly string[], right: readonly string[]): number {
  const a = new Set(left);
  const b = new Set(right);
  if (a.size === 0 && b.size === 0) return 0;
  let intersection = 0;
  for (const value of a) {
    if (b.has(value)) intersection += 1;
  }
  const union = a.size + b.size - intersection;
  if (union === 0) return 0;
  return intersection / union;
}
