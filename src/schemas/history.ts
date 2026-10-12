import { z } from 'zod';

import { topicNameSchema } from './topic.js';

// Consumer group ids admit a wider charset than topic names, so only length
// is constrained here. Do not reuse the topic-name regex.
const groupIdSchema = z.string().min(1).max(255);

export const getLagHistoryShape = {
  groupId: groupIdSchema,
  topic: topicNameSchema,
  partition: z.coerce.number().int().min(0).optional(),
  since: z.string().datetime({ offset: true }).optional(),
  limit: z.coerce.number().int().min(1).max(1000).default(100),
};

export type GetLagHistoryInput = z.output<z.ZodObject<typeof getLagHistoryShape>>;
