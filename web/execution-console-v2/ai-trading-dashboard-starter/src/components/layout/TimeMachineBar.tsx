import { cn } from '@/lib/utils/cn';
import { useTimeMachine, RUN_SNAPSHOTS } from '@/lib/context/TimeMachineContext';

interface Props {
  isOpen: boolean;
}

export default function TimeMachineBar({ isOpen }: Props) {
  const { snapshot, setSnapshot } = useTimeMachine();

  const liveSnap = RUN_SNAPSHOTS[RUN_SNAPSHOTS.length - 1];
  const currentIdx = snapshot
    ? RUN_SNAPSHOTS.findIndex((s) => s.runId === snapshot.runId)
    : RUN_SNAPSHOTS.length - 1;

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const idx = Number(e.target.value);
    const snap = RUN_SNAPSHOTS[idx];
    // Live (last) = no time travel
    setSnapshot(snap.runId === liveSnap.runId ? null : snap);
  }

  if (!isOpen) return null;

  return (
    <div className="border-b border-slate-800 bg-slate-950/90 px-4 py-2.5">
      <div className="flex items-center gap-4">
        <span className="shrink-0 font-mono text-[10px] text-slate-500">
          {RUN_SNAPSHOTS[0].label}
        </span>

        <div className="relative flex-1">
          <input
            type="range"
            min={0}
            max={RUN_SNAPSHOTS.length - 1}
            step={1}
            value={currentIdx}
            onChange={handleChange}
            className="h-1.5 w-full cursor-pointer appearance-none rounded-full bg-slate-700 accent-blue-500"
            aria-label="Scrub to prior run"
          />
          {/* Tick marks */}
          <div className="mt-1 flex justify-between px-0.5">
            {RUN_SNAPSHOTS.map((s, i) => (
              <button
                key={s.runId}
                type="button"
                onClick={() => setSnapshot(s.runId === liveSnap.runId ? null : s)}
                className={cn(
                  'h-1 w-1 rounded-full transition-colors',
                  i === currentIdx ? 'bg-blue-400 scale-150' : 'bg-slate-700 hover:bg-slate-500',
                )}
                title={s.label}
              />
            ))}
          </div>
        </div>

        <span
          className={cn(
            'shrink-0 font-mono text-[11px] font-semibold',
            snapshot ? 'text-amber-300' : 'text-emerald-400',
          )}
        >
          {snapshot ? snapshot.label : '04-15 · live'}
        </span>
      </div>

      {snapshot && (
        <button
          type="button"
          onClick={() => setSnapshot(null)}
          className="mt-1 text-[10px] text-slate-500 hover:text-slate-300 transition-colors"
        >
          ← Return to live
        </button>
      )}
    </div>
  );
}

/** Sticky amber read-only banner shown on every page when time-traveling. */
export function TimeMachineBanner() {
  const { snapshot, setSnapshot } = useTimeMachine();
  if (!snapshot) return null;

  return (
    <div className="mb-4 flex items-center justify-between rounded-xl border border-amber-700/50 bg-amber-500/10 px-4 py-2.5">
      <span className="text-xs text-amber-300">
        ⊙ Viewing <span className="font-semibold">{snapshot.date}</span> · run {snapshot.runId} ·{' '}
        <span className="font-semibold">Read-only</span>
      </span>
      <button
        type="button"
        onClick={() => setSnapshot(null)}
        className="ml-4 rounded-lg border border-amber-700/50 px-2.5 py-1 text-[10px] font-semibold text-amber-300 hover:bg-amber-500/10 transition-colors"
      >
        Return to live
      </button>
    </div>
  );
}
