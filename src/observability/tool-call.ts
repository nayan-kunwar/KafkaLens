import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

const storage = new AsyncLocalStorage<string>();

export function currentToolCallId(): string | undefined {
  return storage.getStore();
}

/** Bindings for a Pino mixin. Empty outside a tool call. */
export function toolCallBindings(): { toolCallId: string } | Record<string, never> {
  const toolCallId = currentToolCallId();
  return toolCallId === undefined ? {} : { toolCallId };
}

export function runToolCall<T>(fn: (toolCallId: string) => Promise<T>): Promise<T> {
  const toolCallId = randomUUID();
  return storage.run(toolCallId, () => fn(toolCallId));
}

/** Debug line emitted next to a metric observation. No-op outside a tool call. */
export function logTiming(
  logger: { debug: (obj: object, msg: string) => void } | undefined,
  event: string,
  fields: Record<string, string | number>,
): void {
  const toolCallId = currentToolCallId();
  if (logger === undefined || toolCallId === undefined) return;
  logger.debug({ ...fields, toolCallId }, event);
}
