import { create } from "zustand";

type ToastMessage = {
  id: number;
  title: string;
  body: string;
};

type ExecutionUiState = {
  selectedTaskId: string | null;
  isTaskDrawerOpen: boolean;
  activeTaskId: string | null;
  progressDismissedForTaskId: string | null;
  selectedRunId: string | null;
  toast: ToastMessage | null;
  selectTask: (taskId: string) => void;
  closeTaskDrawer: () => void;
  setActiveTask: (taskId: string | null) => void;
  dismissProgress: (taskId: string | null) => void;
  resetProgressDismissal: () => void;
  setSelectedRun: (runId: string | null) => void;
  showToast: (title: string, body: string) => void;
  clearToast: () => void;
};

let toastSequence = 0;

export const useExecutionUiStore = create<ExecutionUiState>((set) => ({
  selectedTaskId: null,
  isTaskDrawerOpen: false,
  activeTaskId: null,
  progressDismissedForTaskId: null,
  selectedRunId: null,
  toast: null,
  selectTask: (taskId) => set({ selectedTaskId: taskId, isTaskDrawerOpen: true }),
  closeTaskDrawer: () => set({ isTaskDrawerOpen: false }),
  setActiveTask: (taskId) =>
    set({
      activeTaskId: taskId,
      progressDismissedForTaskId: null,
      selectedTaskId: taskId,
      isTaskDrawerOpen: false,
    }),
  dismissProgress: (taskId) => set({ progressDismissedForTaskId: taskId }),
  resetProgressDismissal: () => set({ progressDismissedForTaskId: null }),
  setSelectedRun: (runId) => set({ selectedRunId: runId }),
  showToast: (title, body) =>
    set({
      toast: {
        id: ++toastSequence,
        title,
        body,
      },
    }),
  clearToast: () => set({ toast: null }),
}));
