export type ErrorCode =
  'CONFIG_INVALID' | 'KAFKA_UNAVAILABLE' | 'AUTH_FAILED' | 'KAFKA_ERROR' | 'NOT_FOUND' | 'INTERNAL';

export class AppError extends Error {
  readonly code: ErrorCode;
  readonly context: Record<string, unknown> | undefined;

  constructor(code: ErrorCode, message: string, context?: Record<string, unknown>) {
    super(message);
    this.name = 'AppError';
    this.code = code;
    this.context = context;
  }

  toJSON(): { code: ErrorCode; message: string; context?: Record<string, unknown> } {
    return this.context === undefined
      ? { code: this.code, message: this.message }
      : { code: this.code, message: this.message, context: this.context };
  }
}
