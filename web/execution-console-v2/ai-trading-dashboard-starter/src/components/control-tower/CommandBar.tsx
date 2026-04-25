/**
 * Global command palette ("⌘ K" / "/").
 *
 * For PR #7 it ships with one capability — tab navigation. PR #12 will
 * add symbol search + recent-runs jumps, but the keyboard shortcut and
 * modal scaffolding are in place now so future additions don't ripple.
 *
 * The keyboard listener lives in :mod:`hooks/useGlobalShortcuts` so the
 * AppLayout can mount it once and the component itself stays stateless
 * about *when* it should open — only *what* it shows.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

import { CommandIcon } from './icons';
import { cn } from '@/lib/utils/cn';

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

interface CommandEntry {
  id: string;
  label: string;
  hint: string;
  to: string;
  keywords: string[];
}

const COMMANDS: readonly CommandEntry[] = [
  {
    id: 'home',
    label: 'Open Control Tower',
    hint: 'home · landing',
    to: '/',
    keywords: ['home', 'landing', 'control', 'tower', 'overview'],
  },
  {
    id: 'pipeline',
    label: 'Open Pipeline',
    hint: 'workspace · stages',
    to: '/pipeline',
    keywords: ['pipeline', 'workspace', 'stages'],
  },
  {
    id: 'ranking',
    label: 'Open Ranking',
    hint: 'top symbols · factors',
    to: '/ranking',
    keywords: ['ranking', 'rank', 'symbols', 'factors', 'composite'],
  },
  {
    id: 'patterns',
    label: 'Open Patterns',
    hint: 'cup · vcp · htf',
    to: '/patterns',
    keywords: ['patterns', 'cup', 'handle', 'vcp', 'flag'],
  },
  {
    id: 'sectors',
    label: 'Open Sectors',
    hint: 'leadership · rotation',
    to: '/sectors',
    keywords: ['sectors', 'leadership', 'rotation', 'heatmap'],
  },
  {
    id: 'execution',
    label: 'Open Execution',
    hint: 'orders · risk',
    to: '/execution',
    keywords: ['execution', 'orders', 'eligible', 'blocked', 'risk'],
  },
  {
    id: 'runs',
    label: 'Open Runs Audit',
    hint: 'history · DQ · artifacts',
    to: '/runs',
    keywords: ['runs', 'history', 'dq', 'artifacts', 'audit'],
  },
  {
    id: 'shadow',
    label: 'Open Shadow',
    hint: 'drift · agreement',
    to: '/shadow',
    keywords: ['shadow', 'drift', 'agreement'],
  },
  {
    id: 'research',
    label: 'Open Research',
    hint: 'experiments · backtests',
    to: '/research',
    keywords: ['research', 'experiments', 'backtests'],
  },
];

function score(command: CommandEntry, query: string): number {
  if (!query) return 1;
  const q = query.trim().toLowerCase();
  if (!q) return 1;
  const haystack = [
    command.label.toLowerCase(),
    command.hint.toLowerCase(),
    ...command.keywords,
  ];
  for (const term of haystack) {
    if (term === q) return 1000;
    if (term.startsWith(q)) return 500;
    if (term.includes(q)) return 100;
  }
  return 0;
}

export default function CommandBar({ isOpen, onClose }: Props) {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);

  // Reset on every open so the user always lands on a clean palette.
  useEffect(() => {
    if (isOpen) {
      setQuery('');
      setActiveIndex(0);
    }
  }, [isOpen]);

  const results = useMemo(() => {
    const ranked = COMMANDS.map((cmd) => ({ cmd, s: score(cmd, query) }))
      .filter((entry) => entry.s > 0)
      .sort((a, b) => b.s - a.s);
    return ranked.map((entry) => entry.cmd);
  }, [query]);

  // Keep the highlighted index inside the result list bounds.
  useEffect(() => {
    if (activeIndex >= results.length && results.length > 0) {
      setActiveIndex(results.length - 1);
    }
  }, [results.length, activeIndex]);

  if (!isOpen) return null;

  function handleKey(event: React.KeyboardEvent<HTMLDivElement>) {
    if (event.key === 'Escape') {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveIndex((idx) => Math.min(idx + 1, Math.max(results.length - 1, 0)));
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveIndex((idx) => Math.max(idx - 1, 0));
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      const cmd = results[activeIndex];
      if (cmd) {
        navigate(cmd.to);
        onClose();
      }
    }
  }

  return (
    <div
      role="dialog"
      aria-label="Command palette"
      className="fixed inset-0 z-50 flex items-start justify-center bg-slate-950/80 px-4 pt-24 backdrop-blur-sm"
      onClick={onClose}
      onKeyDown={handleKey}
    >
      <div
        className={cn(
          'w-full max-w-xl overflow-hidden rounded-2xl border border-slate-700 bg-slate-900',
          'shadow-2xl',
        )}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-slate-800 px-4 py-3">
          <CommandIcon size={18} className="text-blue-400" />
          <input
            autoFocus
            type="text"
            placeholder="Type to navigate… (↑↓ to move, ↵ to open, Esc to close)"
            className="w-full bg-transparent text-sm text-slate-100 placeholder:text-slate-500 focus:outline-none"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>

        <ul className="max-h-80 overflow-y-auto p-2">
          {results.length === 0 ? (
            <li className="px-3 py-4 text-sm text-slate-400">
              No matches for "{query}".
            </li>
          ) : (
            results.map((cmd, idx) => (
              <li key={cmd.id}>
                <button
                  type="button"
                  className={cn(
                    'flex w-full items-center justify-between rounded-md px-3 py-2 text-left text-sm transition-colors',
                    idx === activeIndex
                      ? 'bg-blue-500/15 text-blue-300'
                      : 'text-slate-300 hover:bg-slate-800',
                  )}
                  onMouseEnter={() => setActiveIndex(idx)}
                  onClick={() => {
                    navigate(cmd.to);
                    onClose();
                  }}
                >
                  <span className="font-medium">{cmd.label}</span>
                  <span className="text-[10px] uppercase tracking-wider text-slate-500">
                    {cmd.hint}
                  </span>
                </button>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
}
