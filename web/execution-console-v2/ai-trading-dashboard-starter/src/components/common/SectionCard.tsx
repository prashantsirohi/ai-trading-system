import { useState, type PropsWithChildren, type ReactNode } from 'react';
import { ChevronDownIcon } from '@heroicons/react/24/outline';

import { cn } from '@/lib/utils/cn';

interface Props extends PropsWithChildren {
  title: string;
  description?: string;
  collapsible?: boolean;
  defaultCollapsed?: boolean;
  meta?: ReactNode;
}

export default function SectionCard({
  title,
  description,
  children,
  collapsible = false,
  defaultCollapsed = false,
  meta,
}: Props) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);

  return (
    <section className="rounded-lg border border-slate-800 bg-slate-900 p-4 shadow-soft">
      <div className={cn('flex items-start justify-between gap-3', collapsed ? 'mb-0' : 'mb-3')}>
        <div className="min-w-0">
          <h2 className="text-base font-semibold">{title}</h2>
          {description ? <p className="mt-0.5 text-xs leading-5 text-slate-400">{description}</p> : null}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {meta ? <div className="text-xs text-slate-400">{meta}</div> : null}
          {collapsible ? (
            <button
              type="button"
              aria-label={collapsed ? `Expand ${title}` : `Collapse ${title}`}
              aria-expanded={!collapsed}
              onClick={() => setCollapsed((value) => !value)}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-700 text-slate-300 transition hover:border-slate-500 hover:bg-slate-800 hover:text-slate-100"
            >
              <ChevronDownIcon
                className={cn('h-4 w-4 transition-transform', collapsed ? '-rotate-90' : 'rotate-0')}
                aria-hidden="true"
              />
            </button>
          ) : null}
        </div>
      </div>
      {collapsed ? null : children}
    </section>
  );
}
