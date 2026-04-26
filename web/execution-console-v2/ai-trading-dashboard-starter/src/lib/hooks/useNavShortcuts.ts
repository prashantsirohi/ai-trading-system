/**
 * Global ``g+key`` page-navigation shortcuts (Quantis keyboard map).
 *
 * Sequence: press ``g`` then a second key within 1.5s to jump to the
 * matching route. Ignored while typing in an input / textarea.
 *
 * Map:
 *   g p → /pipeline
 *   g r → /ranking
 *   g s → /sectors
 *   g u → /runs
 *   g w → /watchlist  (proposal #02)
 *   g x → /risk       (proposal #03, placeholder)
 *   g k → /patterns
 *   g y → /shadow
 *   g h → /execution
 */
import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';

const MAP: Record<string, string> = {
  p: '/pipeline',
  r: '/ranking',
  s: '/sectors',
  u: '/runs',
  w: '/watchlist',
  x: '/risk',
  k: '/patterns',
  y: '/shadow',
  h: '/execution',
};

export function useNavShortcuts(): void {
  const navigate = useNavigate();
  const pendingG = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    function handler(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      const isTyping =
        tag === 'input' ||
        tag === 'textarea' ||
        target?.getAttribute('contenteditable') === 'true';
      if (isTyping) return;

      if (event.key === 'g' && !event.metaKey && !event.ctrlKey && !event.altKey) {
        pendingG.current = true;
        if (timer.current) clearTimeout(timer.current);
        timer.current = setTimeout(() => {
          pendingG.current = false;
        }, 1500);
        return;
      }

      if (pendingG.current) {
        pendingG.current = false;
        if (timer.current) clearTimeout(timer.current);
        const route = MAP[event.key.toLowerCase()];
        if (route) {
          event.preventDefault();
          navigate(route);
        }
      }
    }

    window.addEventListener('keydown', handler);
    return () => {
      window.removeEventListener('keydown', handler);
      if (timer.current) clearTimeout(timer.current);
    };
  }, [navigate]);
}
