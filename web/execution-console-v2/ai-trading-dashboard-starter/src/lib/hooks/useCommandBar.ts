/**
 * Global keyboard shortcut for the command palette.
 *
 * Owns ``isOpen`` state and binds ``Cmd+K`` (macOS) / ``Ctrl+K`` / ``/`` to
 * open it. Mounted once at the AppLayout level so every page gets the
 * shortcut without having to opt in.
 *
 * Keys are explicitly *not* bound while the user is typing into an input
 * or textarea — that's the common cause of accidental "/" capture.
 */
import { useCallback, useEffect, useState } from 'react';

interface CommandBarHandle {
  isOpen: boolean;
  open: () => void;
  close: () => void;
  toggle: () => void;
}

export function useCommandBar(): CommandBarHandle {
  const [isOpen, setIsOpen] = useState(false);

  const open = useCallback(() => setIsOpen(true), []);
  const close = useCallback(() => setIsOpen(false), []);
  const toggle = useCallback(() => setIsOpen((current) => !current), []);

  useEffect(() => {
    function handler(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      const isTyping =
        tag === 'input' ||
        tag === 'textarea' ||
        target?.getAttribute('contenteditable') === 'true';

      // Cmd+K / Ctrl+K always works, even inside inputs (it's the standard
      // command-palette shortcut everywhere).
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        toggle();
        return;
      }

      if (isTyping) return;

      if (event.key === '/') {
        event.preventDefault();
        open();
      }
    }

    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, toggle]);

  return { isOpen, open, close, toggle };
}
