import { z } from 'zod';

// Consumer group ids admit a wider charset than topic names (e.g. ':', '/'),
// so only length is constrained here. Do not reuse the topic-name regex.
const groupIdSchema = z.string().min(1).max(255);

export const listConsumerGroupsShape = {
  groupIdContains: z.string().min(1).max(100).optional(),
  protocolType: z.string().min(1).max(100).optional(),
  limit: z.coerce.number().int().min(1).max(500).default(100),
};

export const getConsumerGroupShape = {
  groupId: groupIdSchema,
};

export const getConsumerAssignmentsShape = {
  groupId: groupIdSchema,
};

export type ListConsumerGroupsInput = z.output<z.ZodObject<typeof listConsumerGroupsShape>>;
export type GetConsumerGroupInput = z.output<z.ZodObject<typeof getConsumerGroupShape>>;
export type GetConsumerAssignmentsInput = z.output<z.ZodObject<typeof getConsumerAssignmentsShape>>;
