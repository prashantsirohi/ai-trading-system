import type { SectorRotationRow } from '@/lib/api/sectorRotation';
import { cn } from '@/lib/utils/cn';

export type RRGScaleMode = 'auto' | 'focused' | 'wide';
export type RRGLabelMode = 'top' | 'selected' | 'all' | 'off';

type Props = {
  groups: SectorRotationRow[];
  history: SectorRotationRow[];
  scaleMode: RRGScaleMode;
  labelMode: RRGLabelMode;
  selectedGroupName?: string | null;
  expanded?: boolean;
  onSelect: (group: SectorRotationRow) => void;
};

const quadrantFill: Record<string, string> = {
  Leading: 'fill-emerald-500/10',
  Improving: 'fill-sky-500/10',
  Weakening: 'fill-amber-500/10',
  Lagging: 'fill-rose-500/10',
};

const pointFill: Record<string, string> = {
  Leading: 'fill-emerald-300 stroke-emerald-100',
  Improving: 'fill-sky-300 stroke-sky-100',
  Weakening: 'fill-amber-300 stroke-amber-100',
  Lagging: 'fill-rose-300 stroke-rose-100',
};

export default function RRGChart({
  groups,
  history,
  scaleMode,
  labelMode,
  selectedGroupName,
  expanded = false,
  onSelect,
}: Props) {
  const points = [...groups, ...history].filter(isFinitePoint);
  if (points.length === 0) {
    return (
      <div className="flex min-h-[420px] items-center justify-center rounded-lg border border-dashed border-slate-700 text-sm text-slate-400">
        No RRG points available for the selected view.
      </div>
    );
  }

  const xValues = points.map((point) => toFiniteNumber(point.rs_ratio) ?? 100);
  const yValues = points.map((point) => toFiniteNumber(point.rs_momentum) ?? 100);
  const xDomain = displayDomain(xValues, scaleMode);
  const yDomain = displayDomain(yValues, scaleMode);
  const width = expanded ? 1280 : 900;
  const height = expanded ? 780 : 620;
  const pad = { left: 62, right: 34, top: 26, bottom: 74 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const x100 = xScale(100, xDomain, pad.left, plotW);
  const y100 = yScale(100, yDomain, pad.top, plotH);

  const trails = groupHistory(history);
  const topLabelLimit = 20;
  const labeledGroups = new Set(
    labelMode === 'top'
      ? groups
          .filter(isFinitePoint)
          .sort((a, b) => pointDistance(b) - pointDistance(a))
          .slice(0, topLabelLimit)
          .map(labelFor)
      : [],
  );
  if (selectedGroupName) labeledGroups.add(selectedGroupName);

  return (
    <div className="overflow-hidden rounded-md border border-slate-800 bg-slate-950/70">
      <svg
        data-testid="rrg-chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="RRG-style rotation chart"
        className="w-full"
        style={{ height: expanded ? 'calc(100vh - 150px)' : 'clamp(560px, 66vh, 820px)' }}
      >
        <rect x={pad.left} y={pad.top} width={x100 - pad.left} height={y100 - pad.top} className={quadrantFill.Improving} />
        <rect x={x100} y={pad.top} width={pad.left + plotW - x100} height={y100 - pad.top} className={quadrantFill.Leading} />
        <rect x={pad.left} y={y100} width={x100 - pad.left} height={pad.top + plotH - y100} className={quadrantFill.Lagging} />
        <rect x={x100} y={y100} width={pad.left + plotW - x100} height={pad.top + plotH - y100} className={quadrantFill.Weakening} />

        <text x={(pad.left + x100) / 2} y={(pad.top + y100) / 2} textAnchor="middle" className="fill-sky-100/30 text-3xl font-semibold">Improving</text>
        <text x={(x100 + pad.left + plotW) / 2} y={(pad.top + y100) / 2} textAnchor="middle" className="fill-emerald-100/30 text-3xl font-semibold">Leading</text>
        <text x={(pad.left + x100) / 2} y={(y100 + pad.top + plotH) / 2} textAnchor="middle" className="fill-rose-100/30 text-3xl font-semibold">Lagging</text>
        <text x={(x100 + pad.left + plotW) / 2} y={(y100 + pad.top + plotH) / 2} textAnchor="middle" className="fill-amber-100/30 text-3xl font-semibold">Weakening</text>

        <line data-testid="rrg-crosshair-x" x1={x100} x2={x100} y1={pad.top} y2={pad.top + plotH} stroke="rgb(226 232 240 / 0.78)" strokeWidth={1.8} strokeDasharray="6 5" />
        <line data-testid="rrg-crosshair-y" x1={pad.left} x2={pad.left + plotW} y1={y100} y2={y100} stroke="rgb(226 232 240 / 0.78)" strokeWidth={1.8} strokeDasharray="6 5" />
        <rect x={pad.left} y={pad.top} width={plotW} height={plotH} className="fill-transparent stroke-slate-700" />

        {trails.flatMap((trail) =>
          trail.points.slice(1).map((point, index) => {
            const previous = trail.points[index];
            const age = (index + 1) / Math.max(1, trail.points.length - 1);
            const opacity = 0.04 + age * 0.24;
            return (
              <line
                key={`${trail.name}-${String(point.date ?? index)}`}
                x1={scaledX(previous, xDomain, pad.left, plotW)}
                y1={scaledY(previous, yDomain, pad.top, plotH)}
                x2={scaledX(point, xDomain, pad.left, plotW)}
                y2={scaledY(point, yDomain, pad.top, plotH)}
                stroke="rgb(203 213 225)"
                strokeWidth={1.8}
                strokeOpacity={opacity}
              />
            );
          }),
        )}

        {trails.flatMap((trail) =>
          trail.points.slice(0, -1).map((point, index) => {
            const age = (index + 1) / Math.max(1, trail.points.length);
            return (
              <circle
                key={`${trail.name}-dot-${String(point.date ?? index)}`}
                cx={scaledX(point, xDomain, pad.left, plotW)}
                cy={scaledY(point, yDomain, pad.top, plotH)}
                r={2.2}
                fill="rgb(203 213 225)"
                fillOpacity={0.03 + age * 0.13}
              />
            );
          }),
        )}

        {groups.filter(isFinitePoint).map((group) => {
          const xRaw = toFiniteNumber(group.rs_ratio) ?? 100;
          const yRaw = toFiniteNumber(group.rs_momentum) ?? 100;
          const clamped = xRaw !== clamp(xRaw, xDomain) || yRaw !== clamp(yRaw, yDomain);
          const x = xScale(clamp(xRaw, xDomain), xDomain, pad.left, plotW);
          const y = yScale(clamp(yRaw, yDomain), yDomain, pad.top, plotH);
          const label = labelFor(group);
          const selected = selectedGroupName === label;
          const shouldLabel = labelMode === 'all' || labelMode === 'selected' && selected || labelMode === 'top' && labeledGroups.has(label);
          return (
            <g key={label} data-testid={`rrg-point-${label}`} className="cursor-pointer" onClick={() => onSelect(group)}>
              <title>{tooltip(group)}</title>
              {selected ? <circle cx={x} cy={y} r={14} className="fill-transparent stroke-white/80" strokeWidth={2.5} /> : null}
              {clamped ? <circle cx={x} cy={y} r={12} className="fill-transparent stroke-white/50" strokeDasharray="3 3" strokeWidth={1.5} /> : null}
              <circle cx={x} cy={y} r={selected ? 8.5 : 7} className={cn('stroke-[2.5]', pointFill[group.quadrant ?? 'Lagging'] ?? pointFill.Lagging)} />
              <circle cx={x + 7} cy={y - 7} r={3.2} className="fill-white" />
              {shouldLabel ? (
                <text
                  x={x + 10}
                  y={y - 10}
                  className="fill-slate-100 text-xs font-medium"
                  paintOrder="stroke"
                  stroke="rgb(2 6 23 / 0.85)"
                  strokeWidth={3}
                >
                  {label}
                </text>
              ) : null}
            </g>
          );
        })}

        <text x={pad.left + plotW / 2} y={height - 36} textAnchor="middle" className="fill-slate-300 text-xs">RS Ratio</text>
        <text x={pad.left + plotW / 2} y={height - 18} textAnchor="middle" className="fill-slate-500 text-[11px]">Right = stronger than benchmark</text>
        <text x={20} y={pad.top + plotH / 2} transform={`rotate(-90 20 ${pad.top + plotH / 2})`} textAnchor="middle" className="fill-slate-300 text-xs">RS Momentum</text>
        <text x={40} y={pad.top + plotH / 2} transform={`rotate(-90 40 ${pad.top + plotH / 2})`} textAnchor="middle" className="fill-slate-500 text-[11px]">Up = improving momentum</text>
      </svg>
    </div>
  );
}

function isFinitePoint(point: SectorRotationRow): boolean {
  return toFiniteNumber(point.rs_ratio) !== null && toFiniteNumber(point.rs_momentum) !== null;
}

function displayDomain(values: number[], mode: RRGScaleMode): [number, number] {
  if (mode === 'focused') return [95, 110];
  if (mode === 'wide') return [80, 130];
  return paddedDomain(values);
}

function paddedDomain(values: number[]): [number, number] {
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0) return [95, 110];
  finite.sort((a, b) => a - b);
  const lower = percentile(finite, 0.05);
  const upper = percentile(finite, 0.95);
  const min = Math.min(100, lower);
  const max = Math.max(100, upper);
  const span = Math.max(8, max - min);
  const pad = Math.max(2, span * 0.18);
  return [min - pad, max + pad];
}

function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 1) return sortedValues[0];
  const index = (sortedValues.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function xScale(value: number, domain: [number, number], left: number, width: number): number {
  return left + ((value - domain[0]) / (domain[1] - domain[0] || 1)) * width;
}

function yScale(value: number, domain: [number, number], top: number, height: number): number {
  return top + (1 - (value - domain[0]) / (domain[1] - domain[0] || 1)) * height;
}

function scaledX(point: SectorRotationRow, domain: [number, number], left: number, width: number): number {
  return xScale(clamp(toFiniteNumber(point.rs_ratio) ?? 100, domain), domain, left, width);
}

function scaledY(point: SectorRotationRow, domain: [number, number], top: number, height: number): number {
  return yScale(clamp(toFiniteNumber(point.rs_momentum) ?? 100, domain), domain, top, height);
}

function clamp(value: number, domain: [number, number]): number {
  return Math.min(domain[1], Math.max(domain[0], value));
}

function groupHistory(rows: SectorRotationRow[]) {
  const grouped = new Map<string, SectorRotationRow[]>();
  rows.filter(isFinitePoint).forEach((row) => {
    const key = row.rotation_group_name ?? row.industry ?? row.sector ?? 'Group';
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  });
  return Array.from(grouped.entries()).map(([name, points]) => ({
    name,
    points: points.sort((a, b) => String(a.date ?? '').localeCompare(String(b.date ?? ''))),
  }));
}

function tooltip(group: SectorRotationRow): string {
  const name = labelFor(group);
  return [
    name,
    `Parent: ${group.parent_sector ?? group.sector ?? 'N/A'}`,
    `Quadrant: ${group.quadrant ?? 'Lagging'}`,
    `RS Ratio: ${fmt(group.rs_ratio)}`,
    `Momentum: ${fmt(group.rs_momentum)}`,
    `20D Alpha: ${fmt(group.alpha_20d)}`,
    `20D Return: ${fmt(group.return_20d ?? group.sector_return_20d)}`,
    `Constituents: ${group.constituent_count ?? 'N/A'}`,
  ].join('\n');
}

function labelFor(group: SectorRotationRow): string {
  return group.rotation_group_name ?? group.industry ?? group.sector ?? 'Group';
}

function pointDistance(group: SectorRotationRow): number {
  const x = toFiniteNumber(group.rs_ratio) ?? 100;
  const y = toFiniteNumber(group.rs_momentum) ?? 100;
  return Math.hypot(x - 100, y - 100);
}

function fmt(value: number | null | undefined): string {
  const num = toFiniteNumber(value);
  return num !== null ? num.toFixed(2) : 'N/A';
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}
