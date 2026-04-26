import type { PropsWithChildren } from 'react';
import CommandBar from '@/components/control-tower/CommandBar';
import { useCommandBar } from '@/lib/hooks/useCommandBar';
import { WorkspaceProvider } from '@/components/workspace/WorkspaceContext';
import StockDetailWorkspace from '@/components/workspace/StockDetailWorkspace';
import CompareFactorsModal from '@/components/workspace/CompareFactorsModal';
import CompareTrayLauncher from '@/components/workspace/CompareTrayLauncher';
import Sidebar from './Sidebar';
import TopBar from './TopBar';

export default function AppLayout({ children }: PropsWithChildren) {
  const command = useCommandBar();

  return (
    <WorkspaceProvider>
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <div className="flex min-h-screen">
          <Sidebar />
          <div className="flex-1">
            <TopBar onOpenCommandBar={command.open} />
            <main className="p-4 md:p-6">{children}</main>
          </div>
        </div>
        <CommandBar isOpen={command.isOpen} onClose={command.close} />
        <StockDetailWorkspace />
        <CompareFactorsModal />
        <CompareTrayLauncher />
      </div>
    </WorkspaceProvider>
  );
}
