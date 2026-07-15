import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

// axe-core measures potential icon ligatures through canvas. jsdom intentionally
// omits the 2D implementation, so provide only the deterministic surface axe
// needs instead of emitting a misleading error for every accessibility case.
HTMLCanvasElement.prototype.getContext = (function getContext(this: HTMLCanvasElement) {
  return {
    canvas: this,
    font: '',
    measureText: () => ({ width: 0 }),
  } as unknown as CanvasRenderingContext2D;
} as unknown) as typeof HTMLCanvasElement.prototype.getContext;

// jsdom also reports every pseudo-element style lookup as unimplemented.
// It cannot render pseudo-elements, so use the element's computed style for
// axe's structural checks and leave browser-level rendering checks to Playwright.
const computedStyle = window.getComputedStyle.bind(window);
window.getComputedStyle = ((element: Element) => computedStyle(element)) as typeof window.getComputedStyle;

afterEach(() => cleanup());
