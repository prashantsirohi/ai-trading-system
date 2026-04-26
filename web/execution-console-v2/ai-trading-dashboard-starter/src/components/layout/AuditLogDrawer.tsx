import { cn } from '@/lib/utils/cn';
import { AUDIT_LOG } from '@/lib/mock/operators';

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

export default function AuditLogDrawer({ isOpen, onClose }: Props) {
  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-slate-950/60"
          onClick={onClose}
          aria-hidden="true"
        />
      )}

      {/* Drawer */}
      <aside
        className={cn(
          'fixed right-0 top-0 z-50 flex h-full w-80 flex-col border-l border-slate-800 bg-slate-950 shadow-2xl transition-transform duration-200',
          isOpen ? 'translate-x-0' : 'translate-x-full',
        )}
      >
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div>
            <p className="text-sm font-semibold text-slate-100">Audit Log</p>
            <p className="mt-0.5 text-[10px] uppercase tracking-widest text-slate-500">
              Today · run 1042
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg px-2 py-1 text-slate-400 hover:bg-slate-800 hover:text-white"
            aria-label="Close audit log"
          >
            ✕
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-3 space-y-0 divide-y divide-slate-800/50">
          {AUDIT_LOG.map((entry, i) => (
            <div key={i} className="py-2.5">
              <div className="flex items-baseline gap-2">
                <span className="shrink-0 font-mono text-[10px] text-slate-600">{entry.ts}</span>
                <span
                  className={cn(
                    'text-[11px] font-semibold',
                    entry.kind === 'system' ? 'text-amber-400' : 'text-slate-400',
                  )}
                >
                  {entry.actor}
                </span>
              </div>
              <p className="mt-0.5 text-xs text-slate-300 leading-snug">{entry.msg}</p>
            </div>
          ))}
        </div>

        <div className="border-t border-slate-800 px-4 py-3">
          <p className="text-[10px] text-slate-600 leading-relaxed">
            Audit log is read-only. All actions are recorded on the backend (not wired yet).
          </p>
        </div>
      </aside>
    </>
  );
}
