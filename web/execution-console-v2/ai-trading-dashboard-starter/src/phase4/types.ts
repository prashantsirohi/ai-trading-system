export type JsonRecord = Record<string, unknown>;

export interface ApiErrorBody {
  code: string;
  message: string;
  request_id?: string;
  details?: JsonRecord | null;
}

export interface LineageRef {
  source_type: string;
  source_id: string;
  run_id?: string | null;
  content_hash?: string | null;
  schema_version?: string | null;
  available_at?: string | null;
  policy_version?: string | null;
  source_as_of?: string | null;
}

export interface Freshness {
  source_as_of?: string | null;
  last_successful_run_at?: string | null;
  latest_market_session?: string | null;
  expected_market_session?: string | null;
  staleness_sessions?: number | null;
  freshness_status?: string;
  freshness_reasons?: string[];
}

export interface ResponseMeta {
  request_id: string;
  generated_at: string;
  as_of?: string | null;
  partial: boolean;
  limitations: string[];
  lineage: LineageRef[];
  lineage_meta?: {
    primary?: LineageRef | null;
    supporting?: LineageRef[];
    source_consistent?: boolean;
    source_version_mismatch?: boolean;
  };
  freshness: Freshness;
  pagination?: { next_cursor?: string | null; has_more: boolean; limit: number } | null;
}

export interface ApiEnvelope<T> {
  data: T;
  meta: ResponseMeta;
}

export class Phase4ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: ApiErrorBody,
  ) {
    super(body.message);
    this.name = 'Phase4ApiError';
  }
}

export function asRecord(value: unknown): JsonRecord {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonRecord)
    : {};
}

export function asRecords(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.map(asRecord) : [];
}

export function text(value: unknown, fallback = 'Unavailable'): string {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (Array.isArray(value)) return value.length ? value.map((item) => text(item, '')).join(', ') : fallback;
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}
