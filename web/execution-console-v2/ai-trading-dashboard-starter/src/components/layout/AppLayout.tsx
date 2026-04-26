import type { PropsWithChildren } from 'react';
import CommandBar from '@/components/control-tower/CommandBar';
import { useCommandBar } from '@/lib/hooks/useCommandBar';
import { useNavShortcuts } from '@/lib/hooks/useNavShortcuts';
import { WorkspaceProvider } from '@/components/workspace/WorkspaceContext';
import { TimeMachineProvider } from '@/lib/context/TimeMachineContext';
import { TimeMachineBanner } from '@/components/layout/TimeMachineBar';
import StockDetailWorkspace from '@/components/workspace/StockDetailWorkspace';
import CompareFactorsModal from '@/components/workspace/CompareFactorsModal';
import CompareTrayLauncher from '@/components/workspace/CompareTrayLauncher';
import Sidebar from './Sidebar';
import TopBar from './TopBar';

function NavShortcutsMounter() {
  useNavShortcuts();
  return null;
}

export default function AppLayout({ children }: PropsWithChildren) {
  const command = useCommandBar();

  return (
    <TimeMachineProvider>
      <WorkspaceProvider>
        <NavShortcutsMounter />
        <div className="min-h-screen bg-slate-950 text-slate-100">
          <div className="flex min-h-screen">
            <Sidebar />
            <div className="flex min-h-screen flex-1 flex-col">
              <TopBar onOpenCommandBar={command.open} />
              <main className="flex-1 p-4 md:p-6">
                <TimeMachineBanner />
                {children}
              </main>
            </div>
          </div>
          <CommandBar isOpen={command.isOpen} onClose={command.close} />
          <StockDetailWorkspace />
          <CompareFactorsModal />
          <CompareTrayLauncher />
        </div>
      </WorkspaceProvider>
    </TimeMachineProvider>
  );
}
