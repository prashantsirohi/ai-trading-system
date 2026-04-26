/**
 * Cross-page open/close state for the Stock Detail Workspace + Compare modal.
 *
 * Pages call ``useWorkspace().openWorkspace(symbol)`` (typically from a row
 * click or keyboard shortcut) to drive the modal. The provider also owns the
 * Compare-Factors selection so it survives navigation.
 */
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from 'react';

const COMPARE_LIMIT = 3;

interface WorkspaceContextValue {
  /** Symbol currently shown in the Stock Detail Workspace, or `null` if closed. */
  workspaceSymbol: string | null;
  openWorkspace: (symbol: string) => void;
  closeWorkspace: () => void;

  /** Symbols pinned in the Compare-Factors tray (max 3). */
  compareSymbols: string[];
  toggleCompare: (symbol: string) => void;
  clearCompare: () => void;

  /** Whether the Compare-Factors modal is open. */
  compareOpen: boolean;
  openCompare: () => void;
  closeCompare: () => void;
}

const WorkspaceContext = createContext<WorkspaceContextValue | null>(null);

interface ProviderProps {
  children: ReactNode;
}

export function WorkspaceProvider({ children }: ProviderProps) {
  const [workspaceSymbol, setWorkspaceSymbol] = useState<string | null>(null);
  const [compareSymbols, setCompareSymbols] = useState<string[]>([]);
  const [compareOpen, setCompareOpen] = useState(false);

  const openWorkspace = useCallback((symbol: string) => {
    setWorkspaceSymbol(symbol);
  }, []);

  const closeWorkspace = useCallback(() => {
    setWorkspaceSymbol(null);
  }, []);

  const toggleCompare = useCallback((symbol: string) => {
    setCompareSymbols((prev) => {
      if (prev.includes(symbol)) return prev.filter((s) => s !== symbol);
      if (prev.length >= COMPARE_LIMIT) return prev;
      return [...prev, symbol];
    });
  }, []);

  const clearCompare = useCallback(() => setCompareSymbols([]), []);

  const value = useMemo<WorkspaceContextValue>(
    () => ({
      workspaceSymbol,
      openWorkspace,
      closeWorkspace,
      compareSymbols,
      toggleCompare,
      clearCompare,
      compareOpen,
      openCompare: () => setCompareOpen(true),
      closeCompare: () => setCompareOpen(false),
    }),
    [
      workspaceSymbol,
      openWorkspace,
      closeWorkspace,
      compareSymbols,
      toggleCompare,
      clearCompare,
      compareOpen,
    ],
  );

  return <WorkspaceContext.Provider value={value}>{children}</WorkspaceContext.Provider>;
}

export function useWorkspace(): WorkspaceContextValue {
  const ctx = useContext(WorkspaceContext);
  if (!ctx) {
    throw new Error('useWorkspace must be used inside a <WorkspaceProvider>');
  }
  return ctx;
}

export const COMPARE_LIMIT_VALUE = COMPARE_LIMIT;
