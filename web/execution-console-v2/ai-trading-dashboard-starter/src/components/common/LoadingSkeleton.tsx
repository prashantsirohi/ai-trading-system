interface LoadingSkeletonProps {
  rows?: number;
}

export function TableSkeleton({ rows = 5 }: LoadingSkeletonProps) {
  return (
    <div className="animate-pulse">
      <div className="mb-4 flex gap-4">
        <div className="h-6 w-16 rounded bg-slate-800" />
        <div className="h-6 w-16 rounded bg-slate-800" />
        <div className="h-6 w-16 rounded bg-slate-800" />
        <div className="h-6 w-16 rounded bg-slate-800" />
        <div className="h-6 w-16 rounded bg-slate-800" />
        <div className="h-6 w-16 rounded bg-slate-800" />
      </div>
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="mb-3 flex gap-4 border-b border-slate-800 pb-3"
        >
          <div className="h-4 w-20 rounded bg-slate-800" />
          <div className="h-4 w-16 rounded bg-slate-800" />
          <div className="h-4 w-12 rounded bg-slate-800" />
          <div className="h-4 w-24 rounded bg-slate-800" />
          <div className="h-4 w-20 rounded bg-slate-800" />
          <div className="h-4 w-12 rounded bg-slate-800" />
        </div>
      ))}
    </div>
  );
}

export function CardSkeleton() {
  return (
    <div className="animate-pulse rounded-lg border border-slate-800 bg-slate-900/50 p-6">
      <div className="mb-4 h-6 w-48 rounded bg-slate-800" />
      <div className="space-y-3">
        <div className="h-4 w-full rounded bg-slate-800" />
        <div className="h-4 w-3/4 rounded bg-slate-800" />
        <div className="h-4 w-1/2 rounded bg-slate-800" />
      </div>
    </div>
  );
}