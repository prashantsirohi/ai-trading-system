import { cn } from '@/lib/utils/cn';

export interface ErrorStateProps {
  /** The error to display — a string message or an Error instance. */
  error: string | Error;
  /** Optional retry callback wired to a button below the message. */
  onRetry?: () => void;
  /** Optional override for the retry button label. */
  retryLabel?: string;
  className?: string;
}

/**
 * Standard error callout for failed queries. Pairs with react-query's
 * `isError`/`error`/`refetch` tuple.
 */
export default function ErrorState({
  error,
  onRetry,
  retryLabel = 'Retry',
  className,
}: ErrorStateProps) {
  const message = typeof error === 'string' ? error : error.message;
  return (
    <div className={cn('space-y-3', className)}>
      <p className="text-sm text-rose-300">{message}</p>
      {onRetry ? (
        <button
          type="button"
          onClick={onRetry}
          className="rounded-md border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
        >
          {retryLabel}
        </button>
      ) : null}
    </div>
  );
}
