export type NativeCompactionStep = 'activate' | 'copy' | 'cut' | 'rollback' | 'collect' | 'checkpoint' | 'enable_incremental';

export interface NativeCompactionApply {
  operation_id: string;
  expected_generation: string;
  step: NativeCompactionStep;
  limit: number;
  max_bytes: number;
  max_ms: number;
  lease_owner_id: string;
  lease_fence: number;
}

export type NativeCompactionRequest =
  | { action: 'compact_status' }
  | { action: 'compact_apply'; request: NativeCompactionApply };

export interface NativeCompactionStatus {
  format: 3;
  generation: string;
  active: boolean;
  phase: 'legacy' | 'copy' | 'compact' | 'rollback';
  cursor: { scope: string; key: string };
  rows_complete: boolean;
  vectors_complete: boolean;
  complete: boolean;
  physical_bytes_reclaimed: null;
  operation_id?: string;
  processed_count?: number;
  input_bytes?: number;
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);
const count = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const counter = (value: unknown): value is string => typeof value === 'string' &&
  /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 18_446_744_073_709_551_615n;

export function isNativeCompactionRequest(value: { action: string }): value is NativeCompactionRequest {
  return value.action === 'compact_status' || value.action === 'compact_apply';
}

export function validateNativeCompactionRequest(value: NativeCompactionRequest): void {
  if (value.action === 'compact_status') return;
  const request = value.request;
  if (!record(request) || typeof request.operation_id !== 'string' || !request.operation_id || request.operation_id.length > 128 ||
    !counter(request.expected_generation) || !['activate', 'copy', 'cut', 'rollback', 'collect', 'checkpoint', 'enable_incremental'].includes(request.step) ||
    !count(request.limit) || request.limit < 1 || request.limit > 128 ||
    !count(request.max_bytes) || request.max_bytes < 1024 || request.max_bytes > 16 * 1024 * 1024 ||
    !count(request.max_ms) || request.max_ms < 1 || request.max_ms > (request.step === 'enable_incremental' ? 86_400_000 : request.step === 'checkpoint' ? 25 : 3000) ||
    typeof request.lease_owner_id !== 'string' || !/^[A-Za-z0-9._:-]{1,128}$/.test(request.lease_owner_id) ||
    !count(request.lease_fence) || request.lease_fence < 1) throw new Error('STATE_STORAGE_INVALID_REQUEST');
}

export function validNativeCompactionResponse(request: NativeCompactionRequest, value: unknown): value is NativeCompactionStatus {
  if (!record(value) || value.format !== 3 || !counter(value.generation) || typeof value.active !== 'boolean' ||
    !['legacy', 'copy', 'compact', 'rollback'].includes(String(value.phase)) || !record(value.cursor) ||
    typeof value.cursor.scope !== 'string' || typeof value.cursor.key !== 'string' ||
    typeof value.rows_complete !== 'boolean' || typeof value.vectors_complete !== 'boolean' ||
    typeof value.complete !== 'boolean' || value.physical_bytes_reclaimed !== null) return false;
  return request.action === 'compact_status' || (value.operation_id === request.request.operation_id &&
    count(value.processed_count) && value.processed_count <= request.request.limit * 4 &&
    count(value.input_bytes) && value.input_bytes <= request.request.max_bytes);
}
