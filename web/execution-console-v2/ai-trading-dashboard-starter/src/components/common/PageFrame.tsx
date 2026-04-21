import type { PropsWithChildren } from 'react';
import { motion } from 'framer-motion';

interface Props extends PropsWithChildren {
  title: string;
  description: string;
}

export default function PageFrame({ title, description, children }: Props) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className="space-y-6"
    >
      <div>
        <h1 className="text-3xl font-semibold">{title}</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">{description}</p>
      </div>
      {children}
    </motion.div>
  );
}
