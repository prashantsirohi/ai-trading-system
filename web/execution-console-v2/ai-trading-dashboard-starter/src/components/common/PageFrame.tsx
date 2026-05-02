import type { PropsWithChildren, ReactNode } from 'react';
import { motion } from 'framer-motion';

interface Props extends PropsWithChildren {
  title: string;
  description: string;
  headerAside?: ReactNode;
}

export default function PageFrame({ title, description, headerAside, children }: Props) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className="space-y-4"
    >
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-start">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-slate-400">{description}</p>
        </div>
        {headerAside ? <div className="min-w-0 xl:w-[680px]">{headerAside}</div> : null}
      </div>
      {children}
    </motion.div>
  );
}
