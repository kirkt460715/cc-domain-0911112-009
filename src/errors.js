export class LedgerError extends Error {
  constructor(code, message, details = {}, status = 400) {
    super(message);
    this.name = 'LedgerError';
    this.code = code;
    this.details = details;
    this.status = status;
  }
}
export function fail(code, message, details = {}, status = 400) {
  throw new LedgerError(code, message, details, status);
}
export function requireValue(condition, code, message, details = {}) {
  if (!condition) {
    fail(code, message, details);
  }
}
export function errorDocument(error) {
  if (error instanceof LedgerError) {
    return {
      error: error.code,
      message: error.message,
      details: error.details,
    };
  }
  return {
    error: 'INTERNAL',
    message: 'Internal operation failed',
    details: {},
  };
}
