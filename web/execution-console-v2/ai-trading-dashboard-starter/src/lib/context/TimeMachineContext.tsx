import { createContext, useContext, useState, type PropsWithChildren } from 'react';
import { RUN_SNAPSHOTS, type RunSnapshot } from '@/lib/mock/operators';

interface TimeMachineState {
  snapshot: RunSnapshot | null;
  isTimeTraveling: boolean;
  setSnapshot: (s: RunSnapshot | null) => void;
}

const TimeMachineContext = createContext<TimeMachineState>({
  snapshot: null,
  isTimeTraveling: false,
  setSnapshot: () => {},
});

export function TimeMachineProvider({ children }: PropsWithChildren) {
  const [snapshot, setSnapshot] = useState<RunSnapshot | null>(null);
  return (
    <TimeMachineContext.Provider
      value={{ snapshot, isTimeTraveling: snapshot !== null, setSnapshot }}
    >
      {children}
    </TimeMachineContext.Provider>
  );
}

export function useTimeMachine() {
  return useContext(TimeMachineContext);
}

export { RUN_SNAPSHOTS };
