/**
 * Tiny inline SVG glyphs for the pattern types surfaced on PatternsPage.
 *
 * The Canvas design calls out specific shapes for Cup & Handle, VCP, and
 * High-Tight-Flag, with a generic fallback for everything else (Round
 * Bottom, Flat Base, Tight Flag, etc.). They're intentionally tiny — no
 * extra icon dependency.
 */
import type { SVGProps } from 'react';

interface IconProps extends Omit<SVGProps<SVGSVGElement>, 'width' | 'height'> {
  size?: number;
}

function asProps({ size = 32, ...rest }: IconProps): SVGProps<SVGSVGElement> {
  return {
    width: size,
    height: size,
    viewBox: '0 0 48 28',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth: 2,
    strokeLinecap: 'round',
    strokeLinejoin: 'round',
    ...rest,
  };
}

export function CupHandleIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      {/* Cup curve */}
      <path d="M2 6 C2 22, 22 22, 22 6" />
      {/* Handle */}
      <path d="M22 6 L30 10 L36 8" />
      {/* Breakout target */}
      <path d="M36 8 L46 4" strokeDasharray="2 2" />
    </svg>
  );
}

export function VcpIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      {/* Volatility contraction — descending range */}
      <path d="M2 4 L2 22" />
      <path d="M12 8 L12 20" />
      <path d="M22 11 L22 17" />
      <path d="M32 13 L32 16" />
      {/* Breakout */}
      <path d="M32 14 L46 6" strokeDasharray="2 2" />
    </svg>
  );
}

export function HighTightFlagIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      {/* Pole */}
      <path d="M4 26 L18 4" />
      {/* Flag */}
      <path d="M18 4 L34 8 L34 16 L18 12 Z" />
      {/* Continuation */}
      <path d="M34 12 L46 8" strokeDasharray="2 2" />
    </svg>
  );
}

export function RoundBottomIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      <path d="M2 6 C 8 26, 28 26, 34 6" />
      <path d="M34 6 L46 4" strokeDasharray="2 2" />
    </svg>
  );
}

export function FlatBaseIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      <path d="M2 16 L34 16" />
      <path d="M2 12 L34 12" strokeDasharray="2 2" />
      <path d="M34 14 L46 6" />
    </svg>
  );
}

export function TightFlagIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      <path d="M4 22 L14 6" />
      <path d="M14 6 L28 9 L28 13 L14 10 Z" />
      <path d="M28 11 L46 6" strokeDasharray="2 2" />
    </svg>
  );
}

export function GenericPatternIcon(props: IconProps) {
  return (
    <svg {...asProps(props)} aria-hidden="true">
      <path d="M2 18 L12 14 L22 16 L32 8 L46 6" />
    </svg>
  );
}

export function patternIconFor(pattern: string | null | undefined) {
  const norm = (pattern ?? '').toLowerCase();
  if (norm.includes('cup')) return CupHandleIcon;
  if (norm.includes('vcp')) return VcpIcon;
  if (norm.includes('high tight') || norm.includes('high-tight')) return HighTightFlagIcon;
  if (norm.includes('round')) return RoundBottomIcon;
  if (norm.includes('flat base')) return FlatBaseIcon;
  if (norm.includes('flag')) return TightFlagIcon;
  return GenericPatternIcon;
}
