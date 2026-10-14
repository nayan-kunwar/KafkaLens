import { z } from 'zod';

import { FINDING_TYPES } from '../domain/findings.js';

import { topicNameSchema } from './topic.js';

const groupIdSchema = z.string().min(1).max(255);

export const diagnoseConsumerGroupShape = {
  groupId: groupIdSchema,
  topic: topicNameSchema,
};

export const findSimilarIncidentsShape = {
  groupId: groupIdSchema,
  topic: topicNameSchema,
  findingTypes: z.array(z.enum(FINDING_TYPES)).max(FINDING_TYPES.length).optional(),
  limit: z.coerce.number().int().min(1).max(50).default(10),
};

export type DiagnoseConsumerGroupInput = z.output<z.ZodObject<typeof diagnoseConsumerGroupShape>>;
export type FindSimilarIncidentsInput = z.output<z.ZodObject<typeof findSimilarIncidentsShape>>;
