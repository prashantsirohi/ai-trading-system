import type { PropsWithChildren } from 'react';

interface Props extends PropsWithChildren {
  title: string;
  description?: string;
}

export default function SectionCard({ title, description, children }: Props) {
  return (
    <section className="rounded-lg border border-slate-800 bg-slate-900 p-4 shadow-soft">
      <div className="mb-3">
        <h2 className="text-base font-semibold">{title}</h2>
        {description ? <p className="mt-0.5 text-xs leading-5 text-slate-400">{description}</p> : null}
      </div>
      {children}
    </section>
  );
}
