import { titleCase } from '@/lib/utils/text';
import { usePipelineWorkspace, useRefreshAll } from '@/lib/queries';

export default function TopBar() {
  const refreshAll = useRefreshAll();
  const { data } = usePipelineWorkspace();

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
            void refreshAll();
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
