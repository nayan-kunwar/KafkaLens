import { z } from 'zod';

import { topicNameSchema } from './topic.js';

// Consumer group ids admit a wider charset than topic names, so only length
// is constrained here. Do not reuse the topic-name regex.
const groupIdSchema = z.string().min(1).max(255);

export const getConsumerLagShape = {
  groupId: groupIdSchema,
  topic: topicNameSchema.optional(),
};

export type GetConsumerLagInput = z.output<z.ZodObject<typeof getConsumerLagShape>>;
