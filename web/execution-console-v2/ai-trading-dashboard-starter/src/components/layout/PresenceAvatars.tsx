import { useState } from 'react';
import { cn } from '@/lib/utils/cn';
import { OPERATORS, type OperatorPresence } from '@/lib/mock/operators';

function avatarTitle(op: OperatorPresence): string {
  if (op.page) return `${op.name} viewing ${op.page} · ${op.idleMins}m`;
  return `${op.name} idle · ${op.idleMins}m`;
}

export default function PresenceAvatars() {
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  return (
    <div className="flex items-center gap-1.5">
      {OPERATORS.map((op, i) => (
        <div key={op.initials} className="relative">
          <button
            type="button"
            title={avatarTitle(op)}
            onMouseEnter={() => setHoveredIdx(i)}
            onMouseLeave={() => setHoveredIdx(null)}
            className={cn(
              'flex h-7 w-7 items-center justify-center rounded-full text-[10px] font-bold text-white transition-transform hover:scale-110',
              op.page ? 'ring-2 ring-offset-1 ring-offset-slate-950 ring-emerald-500' : '',
            )}
            style={{ backgroundColor: op.color }}
          >
            {op.initials}
          </button>

          {hoveredIdx === i && (
            <div className="absolute left-1/2 top-full z-50 mt-2 w-44 -translate-x-1/2 rounded-xl border border-slate-700 bg-slate-900 px-3 py-2 shadow-lg">
              <p className="font-semibold text-slate-100 text-[11px]">{op.name}</p>
              <p className="mt-0.5 text-[10px] text-slate-400">
                {op.page ? `Viewing ${op.page}` : 'Idle'} · {op.idleMins}m ago
              </p>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
