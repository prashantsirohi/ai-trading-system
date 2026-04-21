import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getPipelineWorkspace } from '@/lib/api/pipeline';

function titleCase(value: string): string {
  if (!value) {
    return 'Unknown';
  }
  return value
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

export default function TopBar() {
  const queryClient = useQueryClient();
  const { data } = useQuery({
    queryKey: ['pipeline-workspace'],
    queryFn: getPipelineWorkspace,
  });

  const summaryText = data
    ? `${data.runId} • Trust ${titleCase(data.trust)} • ${data.date}`
    : 'Live workspace status';

  return (
    <header className="flex h-16 items-center justify-between border-b border-slate-800 bg-slate-950 px-4 md:px-6">
      <div className="text-sm text-slate-400">{summaryText}</div>
      <div className="flex gap-2">
        <button
          type="button"
          onClick={() => {
            void queryClient.invalidateQueries({ queryKey: ['pipeline-workspace'] });
            void queryClient.invalidateQueries({ queryKey: ['ranking'] });
          }}
          className="rounded-2xl border border-slate-700 bg-slate-900 px-4 py-2 text-sm text-slate-100"
        >
          Refresh
        </button>
        <button
          type="button"
          disabled
          className="rounded-2xl bg-blue-600/80 px-4 py-2 text-sm font-medium text-white opacity-70"
          title="Retry publish wiring is not enabled in this React console yet."
        >
          Retry Publish
        </button>
      </div>
    </header>
  );
}
