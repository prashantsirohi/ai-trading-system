import type { PropsWithChildren } from 'react';

interface Props extends PropsWithChildren {
  title: string;
  description?: string;
}

export default function SectionCard({ title, description, children }: Props) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow-soft">
      <div className="mb-4">
        <h2 className="text-lg font-semibold">{title}</h2>
        {description ? <p className="mt-1 text-sm text-slate-400">{description}</p> : null}
      </div>
      {children}
    </section>
  );
}
