import type { ReactNode } from 'react';
import { cn } from '@/lib/utils/cn';

export interface EmptyStateProps {
  /** Short, sentence-case description of why the section is empty. */
  message: ReactNode;
  /** Optional action button (e.g. "Run pipeline", "Refresh"). */
  action?: ReactNode;
  className?: string;
}

/**
 * Standard empty-data callout used inside SectionCards. Renders a muted
 * rounded panel matching the rest of the dashboard's card chrome.
 */
export default function EmptyState({ message, action, className }: EmptyStateProps) {
  return (
    <div
      className={cn(
        'rounded-xl border border-slate-800 bg-slate-950/40 p-4 text-sm text-slate-300',
        className,
      )}
    >
      <p>{message}</p>
      {action ? <div className="mt-3">{action}</div> : null}
    </div>
  );
}
