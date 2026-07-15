import { createContext, useContext, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { CONFIGURED_API_KEY, CONFIGURED_AUTH_MODE, type AuthMode } from './api';

interface AuthState {
  credential: string;
  authMode: AuthMode;
  signIn: (credential: string, mode: AuthMode) => void;
  signOut: () => void;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [credential, setCredential] = useState(CONFIGURED_API_KEY);
  const [authMode, setAuthMode] = useState<AuthMode>(CONFIGURED_AUTH_MODE);
  const value = useMemo<AuthState>(() => ({
    credential,
    authMode,
    signIn: (next, mode) => { setCredential(next.trim()); setAuthMode(mode); },
    signOut: () => setCredential(''),
  }), [credential, authMode]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used inside AuthProvider');
  return context;
}

export function LoginView() {
  const auth = useAuth();
  const [credential, setCredential] = useState('');
  const [mode, setMode] = useState<AuthMode>(CONFIGURED_AUTH_MODE);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (credential.trim()) auth.signIn(credential, mode);
  };
  return (
    <main className="login-shell">
      <form className="login-card" onSubmit={submit} aria-labelledby="login-title">
        <p className="eyebrow">Phase 4B · Read only</p>
        <h1 id="login-title">Operator dashboard</h1>
        <p>Enter a Phase 4A credential. It is kept only in page memory and is cleared on sign-out or reload.</p>
        <label htmlFor="credential">API credential</label>
        <input id="credential" type="password" autoComplete="off" value={credential} onChange={(e) => setCredential(e.target.value)} />
        <label htmlFor="auth-mode">Authentication mode</label>
        <select id="auth-mode" value={mode} onChange={(e) => setMode(e.target.value as AuthMode)}>
          <option value="bearer">Bearer token</option>
          <option value="api-key">API key header</option>
        </select>
        <button className="primary" type="submit" disabled={!credential.trim()}>Open read-only dashboard</button>
        <p className="muted">Credentials are never placed in URLs, persisted to local storage, or displayed after sign-in.</p>
      </form>
    </main>
  );
}
