import { z } from 'zod';

const topicNameSchema = z
  .string()
  .min(1)
  .max(249)
  .regex(/^[a-zA-Z0-9._-]+$/, 'must contain only letters, digits, ".", "_", and "-"')
  .refine((name) => name !== '.' && name !== '..', 'must not be "." or ".."');

export const listTopicsShape = {
  nameContains: z.string().min(1).max(100).optional(),
  limit: z.coerce.number().int().min(1).max(500).default(100),
};

export const getTopicMetadataShape = {
  topic: topicNameSchema,
};

export const getPartitionInfoShape = {
  topic: topicNameSchema,
  partition: z.coerce.number().int().min(0).optional(),
};

export type ListTopicsInput = z.output<z.ZodObject<typeof listTopicsShape>>;
export type GetTopicMetadataInput = z.output<z.ZodObject<typeof getTopicMetadataShape>>;
export type GetPartitionInfoInput = z.output<z.ZodObject<typeof getPartitionInfoShape>>;
