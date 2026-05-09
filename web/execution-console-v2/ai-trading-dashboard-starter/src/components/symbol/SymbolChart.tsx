/**
 * Professional candlestick chart for the canonical stock page.
 *
 * OHLC candles, volume, SMA 50/200, VWAP, delivery overlay, responsive sizing,
 * timeframe controls, layer toggles, and a compact legend.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { ArrowsPointingInIcon, ArrowsPointingOutIcon } from '@heroicons/react/24/outline';
import {
  ColorType,
  CrosshairMode,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  createChart,
  createSeriesMarkers,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type LineData,
  type SeriesMarker,
  type Time,
} from 'lightweight-charts';
import type { StockOhlcv } from '@/lib/api/stocks';
import { deriveMAs } from '@/lib/symbol/derive';
import { cn } from '@/lib/utils/cn';

type Period = '3M' | '6M' | '1Y' | 'All';
type ChartLayer = 'volume' | 'ma50' | 'ma200' | 'vwap' | 'delivery' | 'patterns';

export interface ChartPatternOverlay {
  family: string | null;
  state: string | null;
  setupQuality?: number | null;
  pivotPrice?: number | null;
  breakoutLevel?: number | null;
  invalidationPrice?: number | null;
  signalDate?: string | null;
  startDate?: string | null;
  endDate?: string | null;
}

const PERIODS: Period[] = ['3M', '6M', '1Y', 'All'];
const LAYERS: Array<{ key: ChartLayer; label: string; tone: string }> = [
  { key: 'volume', label: 'Volume', tone: 'bg-sky-500' },
  { key: 'ma50', label: 'SMA50', tone: 'bg-amber-400' },
  { key: 'ma200', label: 'SMA200', tone: 'bg-blue-400' },
  { key: 'vwap', label: 'VWAP', tone: 'bg-fuchsia-400' },
  { key: 'delivery', label: 'Delivery', tone: 'bg-emerald-400' },
  { key: 'patterns', label: 'Patterns', tone: 'bg-rose-400' },
];
const PERIOD_LIMIT: Record<Period, number> = {
  '3M': 63,
  '6M': 126,
  '1Y': 252,
  All: Number.POSITIVE_INFINITY,
};

function toTime(iso: string): Time {
  return iso.slice(0, 10) as Time;
}

function fmt(value: number | null | undefined, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return value.toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtVolume(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  if (value >= 1e7) return `${(value / 1e7).toFixed(2)}Cr`;
  if (value >= 1e5) return `${(value / 1e5).toFixed(2)}L`;
  return value.toLocaleString('en-IN');
}

function average(values: Array<number | null | undefined>): number | null {
  const clean = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (clean.length === 0) return null;
  return clean.reduce((sum, value) => sum + value, 0) / clean.length;
}

interface Props {
  data: StockOhlcv | null | undefined;
  isLoading: boolean;
  pattern?: ChartPatternOverlay | null;
  breakoutDate?: string | null;
}

export default function SymbolChart({ data, isLoading, pattern }: Props) {
  const chartEl = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const ma50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ma200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const vwapSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const deliverySeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const markerApiRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const patternPriceLinesRef = useRef<IPriceLine[]>([]);

  const [period, setPeriod] = useState<Period>('1Y');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [layers, setLayers] = useState<Record<ChartLayer, boolean>>({
    volume: true,
    ma50: true,
    ma200: true,
    vwap: false,
    delivery: false,
    patterns: true,
  });
  const [legend, setLegend] = useState<{
    date: string;
    open: number | null;
    high: number | null;
    low: number | null;
    close: number | null;
    volume: number | null;
    deliveryPct: number | null;
    vwap: number | null;
  } | null>(null);

  const prepared = useMemo(() => {
    if (!data?.available || data.candles.length === 0) {
      return { candles: [], volumes: [], ma50: [], ma200: [], vwap: [], delivery: [], stats: null, timeSet: new Set<string>() };
    }

    const limit = PERIOD_LIMIT[period];
    const sliced = Number.isFinite(limit) ? data.candles.slice(-limit) : data.candles;
    const { ma50, ma200 } = deriveMAs(sliced);

    const candles: CandlestickData[] = [];
    const volumes: HistogramData[] = [];
    const ma50Line: LineData[] = [];
    const ma200Line: LineData[] = [];
    const vwapLine: LineData[] = [];
    const deliveryLine: LineData[] = [];
    let cumulativePriceVolume = 0;
    let cumulativeVolume = 0;

    sliced.forEach((candle, index) => {
      if (
        !candle.timestamp ||
        candle.open == null ||
        candle.high == null ||
        candle.low == null ||
        candle.close == null
      ) {
        return;
      }
      const time = toTime(candle.timestamp);
      const up = candle.close >= candle.open;
      candles.push({
        time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
      });
      volumes.push({
        time,
        value: candle.volume ?? 0,
        color: up ? 'rgba(16, 185, 129, 0.32)' : 'rgba(244, 63, 94, 0.32)',
      });
      const volume = candle.volume ?? 0;
      if (volume > 0) {
        const typicalPrice = (candle.high + candle.low + candle.close) / 3;
        cumulativePriceVolume += typicalPrice * volume;
        cumulativeVolume += volume;
        vwapLine.push({ time, value: +(cumulativePriceVolume / cumulativeVolume).toFixed(2) });
      }
      if (candle.deliveryPct != null) deliveryLine.push({ time, value: candle.deliveryPct });
      if (ma50[index] != null) ma50Line.push({ time, value: ma50[index] as number });
      if (ma200[index] != null) ma200Line.push({ time, value: ma200[index] as number });
    });

    const closes = sliced.map((candle) => candle.close).filter((value): value is number => value != null && Number.isFinite(value));
    const firstClose = closes[0] ?? null;
    const lastClose = closes[closes.length - 1] ?? null;
    const high = closes.length ? Math.max(...closes) : null;
    const low = closes.length ? Math.min(...closes) : null;
    const stats = {
      changePct: firstClose && lastClose ? ((lastClose - firstClose) / firstClose) * 100 : null,
      high,
      low,
      rangePct: high && low ? ((high - low) / low) * 100 : null,
      avgVolume: average(sliced.map((candle) => candle.volume)),
      avgDelivery: average(sliced.map((candle) => candle.deliveryPct)),
    };

    return {
      candles,
      volumes,
      ma50: ma50Line,
      ma200: ma200Line,
      vwap: vwapLine,
      delivery: deliveryLine,
      stats,
      timeSet: new Set(candles.map((candle) => String(candle.time))),
    };
  }, [data, period]);

  const patternOverlay = useMemo(() => buildPatternOverlay(pattern, prepared.candles, prepared.timeSet), [pattern, prepared.candles, prepared.timeSet]);

  const toggleLayer = (key: ChartLayer) => {
    setLayers((current) => ({ ...current, [key]: !current[key] }));
  };

  useEffect(() => {
    if (!chartEl.current) return undefined;

    const chart = createChart(chartEl.current, {
      autoSize: true,
      height: 390,
      layout: {
        background: { type: ColorType.Solid, color: '#020617' },
        textColor: '#94a3b8',
        fontFamily: 'Inter, ui-sans-serif, system-ui, sans-serif',
      },
      grid: {
        vertLines: { color: 'rgba(30, 41, 59, 0.55)' },
        horzLines: { color: 'rgba(30, 41, 59, 0.55)' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: 'rgba(148, 163, 184, 0.4)', labelBackgroundColor: '#1e293b' },
        horzLine: { color: 'rgba(148, 163, 184, 0.4)', labelBackgroundColor: '#1e293b' },
      },
      rightPriceScale: {
        borderColor: 'rgba(51, 65, 85, 0.75)',
        scaleMargins: { top: 0.08, bottom: 0.26 },
      },
      timeScale: {
        borderColor: 'rgba(51, 65, 85, 0.75)',
        timeVisible: false,
      },
      localization: {
        priceFormatter: (price: number) => price.toFixed(2),
      },
    });

    const candles = chart.addSeries(CandlestickSeries, {
      upColor: '#10b981',
      downColor: '#f43f5e',
      borderUpColor: '#34d399',
      borderDownColor: '#fb7185',
      wickUpColor: '#34d399',
      wickDownColor: '#fb7185',
      priceLineColor: '#38bdf8',
    });
    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.78, bottom: 0 },
      borderVisible: false,
    });
    const ma50 = chart.addSeries(LineSeries, {
      color: '#f59e0b',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const ma200 = chart.addSeries(LineSeries, {
      color: '#60a5fa',
      lineWidth: 1,
      lineStyle: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const vwap = chart.addSeries(LineSeries, {
      color: '#e879f9',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const delivery = chart.addSeries(LineSeries, {
      color: '#34d399',
      lineWidth: 1,
      priceScaleId: 'delivery',
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale('delivery').applyOptions({
      scaleMargins: { top: 0.08, bottom: 0.78 },
      borderVisible: false,
      visible: false,
    });

    chartRef.current = chart;
    candleSeriesRef.current = candles;
    volumeSeriesRef.current = volume;
    ma50SeriesRef.current = ma50;
    ma200SeriesRef.current = ma200;
    vwapSeriesRef.current = vwap;
    deliverySeriesRef.current = delivery;
    markerApiRef.current = createSeriesMarkers(candles, [], { zOrder: 'top' });

    chart.subscribeCrosshairMove((param) => {
      const candle = param.seriesData.get(candles) as CandlestickData | undefined;
      const vol = param.seriesData.get(volume) as HistogramData | undefined;
      const currentVwap = param.seriesData.get(vwap) as LineData | undefined;
      const currentDelivery = param.seriesData.get(delivery) as LineData | undefined;
      if (!candle) {
        const last = prepared.candles[prepared.candles.length - 1];
        const lastVol = prepared.volumes[prepared.volumes.length - 1];
        const lastVwap = prepared.vwap[prepared.vwap.length - 1];
        const lastDelivery = prepared.delivery[prepared.delivery.length - 1];
        setLegend(last ? {
          date: String(last.time),
          open: last.open,
          high: last.high,
          low: last.low,
          close: last.close,
          volume: lastVol?.value ?? null,
          deliveryPct: lastDelivery?.value ?? null,
          vwap: lastVwap?.value ?? null,
        } : null);
        return;
      }
      setLegend({
        date: String(candle.time),
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: vol?.value ?? null,
        deliveryPct: currentDelivery?.value ?? null,
        vwap: currentVwap?.value ?? null,
      });
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ma50SeriesRef.current = null;
      ma200SeriesRef.current = null;
      vwapSeriesRef.current = null;
      deliverySeriesRef.current = null;
      markerApiRef.current = null;
      patternPriceLinesRef.current = [];
    };
  }, []);

  useEffect(() => {
    candleSeriesRef.current?.setData(prepared.candles);
    volumeSeriesRef.current?.setData(prepared.volumes);
    ma50SeriesRef.current?.setData(prepared.ma50);
    ma200SeriesRef.current?.setData(prepared.ma200);
    vwapSeriesRef.current?.setData(prepared.vwap);
    deliverySeriesRef.current?.setData(prepared.delivery);
    chartRef.current?.timeScale().fitContent();

    const last = prepared.candles[prepared.candles.length - 1];
    const lastVol = prepared.volumes[prepared.volumes.length - 1];
    const lastVwap = prepared.vwap[prepared.vwap.length - 1];
    const lastDelivery = prepared.delivery[prepared.delivery.length - 1];
    setLegend(last ? {
      date: String(last.time),
      open: last.open,
      high: last.high,
      low: last.low,
      close: last.close,
      volume: lastVol?.value ?? null,
      deliveryPct: lastDelivery?.value ?? null,
      vwap: lastVwap?.value ?? null,
    } : null);
  }, [prepared]);

  useEffect(() => {
    volumeSeriesRef.current?.applyOptions({ visible: layers.volume });
    ma50SeriesRef.current?.applyOptions({ visible: layers.ma50 });
    ma200SeriesRef.current?.applyOptions({ visible: layers.ma200 });
    vwapSeriesRef.current?.applyOptions({ visible: layers.vwap });
    deliverySeriesRef.current?.applyOptions({ visible: layers.delivery });
    chartRef.current?.priceScale('delivery').applyOptions({ visible: layers.delivery });
  }, [layers]);

  useEffect(() => {
    const series = candleSeriesRef.current;
    if (!series || !markerApiRef.current) return;
    patternPriceLinesRef.current.forEach((line) => series.removePriceLine(line));
    patternPriceLinesRef.current = [];

    if (!layers.patterns || !patternOverlay) {
      markerApiRef.current.setMarkers([]);
      return;
    }

    markerApiRef.current.setMarkers(patternOverlay.markers);
    patternPriceLinesRef.current = patternOverlay.lines.map((line) => series.createPriceLine(line));
  }, [layers.patterns, patternOverlay]);

  useEffect(() => {
    chartRef.current?.applyOptions({ height: isFullscreen ? Math.max(window.innerHeight - 210, 520) : 390 });
    window.setTimeout(() => chartRef.current?.timeScale().fitContent(), 0);
  }, [isFullscreen]);

  useEffect(() => {
    if (!isFullscreen) return undefined;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setIsFullscreen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isFullscreen]);

  if (isLoading) {
    return <div className="flex h-80 items-center justify-center text-sm text-slate-500">Loading chart...</div>;
  }
  if (!data?.available || prepared.candles.length === 0) {
    return <div className="flex h-80 items-center justify-center text-sm text-slate-500">No price history available.</div>;
  }

  const patternSummary = patternOverlay?.summary ?? null;

  return (
    <div className={cn(
      'space-y-2',
      isFullscreen ? 'fixed inset-3 z-50 overflow-y-auto rounded-lg border border-slate-700 bg-slate-950 p-3 shadow-2xl' : '',
    )}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs">
          <span className="font-semibold text-slate-200">Candles</span>
          <span className="text-slate-500">{legend?.date ?? '-'}</span>
          <LegendItem label="O" value={fmt(legend?.open)} />
          <LegendItem label="H" value={fmt(legend?.high)} tone="text-emerald-300" />
          <LegendItem label="L" value={fmt(legend?.low)} tone="text-rose-300" />
          <LegendItem label="C" value={fmt(legend?.close)} />
          <LegendItem label="Vol" value={fmtVolume(legend?.volume)} />
          {layers.vwap ? <LegendItem label="VWAP" value={fmt(legend?.vwap)} tone="text-fuchsia-300" /> : null}
          {layers.delivery ? <LegendItem label="Del" value={`${fmt(legend?.deliveryPct)}%`} tone="text-emerald-300" /> : null}
          {layers.ma50 ? <span className="text-amber-300">SMA50</span> : null}
          {layers.ma200 ? <span className="text-blue-300">SMA200</span> : null}
          {layers.patterns && patternSummary ? <span className="text-rose-300">{patternSummary.family}</span> : null}
        </div>
        <div className="flex items-center gap-1.5">
          <div className="flex overflow-hidden rounded-md border border-slate-700 bg-slate-950/60">
            {PERIODS.map((item) => (
              <button
                key={item}
                type="button"
                onClick={() => setPeriod(item)}
                className={cn(
                  'px-2.5 py-1 text-[11px] font-semibold transition-colors',
                  period === item ? 'bg-slate-800 text-white' : 'text-slate-500 hover:text-slate-300',
                )}
              >
                {item}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={() => setIsFullscreen((current) => !current)}
            className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-slate-700 bg-slate-950/60 text-slate-400 transition-colors hover:text-slate-100"
            title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen chart'}
            aria-label={isFullscreen ? 'Exit fullscreen chart' : 'Open chart fullscreen'}
          >
            {isFullscreen ? <ArrowsPointingInIcon className="h-4 w-4" /> : <ArrowsPointingOutIcon className="h-4 w-4" />}
          </button>
        </div>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap gap-1.5">
          {LAYERS.map((layer) => (
            <button
              key={layer.key}
              type="button"
              onClick={() => toggleLayer(layer.key)}
              className={cn(
                'flex items-center gap-1.5 rounded-md border px-2 py-1 text-[11px] font-semibold transition-colors',
                layers[layer.key]
                  ? 'border-slate-600 bg-slate-800 text-slate-100'
                  : 'border-slate-800 bg-slate-950/40 text-slate-500 hover:text-slate-300',
              )}
              title={`Toggle ${layer.label}`}
            >
              <span className={cn('h-1.5 w-1.5 rounded-full', layer.tone)} />
              {layer.label}
            </button>
          ))}
        </div>
        <div className="grid grid-cols-2 gap-1.5 text-[11px] sm:flex">
          <StatChip label="Period" value={prepared.stats?.changePct == null ? '-' : `${prepared.stats.changePct >= 0 ? '+' : ''}${prepared.stats.changePct.toFixed(2)}%`} tone={prepared.stats?.changePct != null && prepared.stats.changePct >= 0 ? 'text-emerald-300' : 'text-rose-300'} />
          <StatChip label="High" value={fmt(prepared.stats?.high)} tone="text-emerald-300" />
          <StatChip label="Low" value={fmt(prepared.stats?.low)} tone="text-rose-300" />
          <StatChip label="Avg Del" value={prepared.stats?.avgDelivery == null ? '-' : `${prepared.stats.avgDelivery.toFixed(1)}%`} tone="text-slate-200" />
          <StatChip label="Avg Vol" value={fmtVolume(prepared.stats?.avgVolume)} tone="text-slate-200" />
        </div>
      </div>
      {layers.patterns && patternSummary ? (
        <div className="grid gap-1.5 text-[11px] sm:grid-cols-4">
          <StatChip label="Pattern" value={patternSummary.family ?? '-'} tone="text-rose-200" />
          <StatChip label="State" value={patternSummary.state ?? '-'} tone="text-slate-200" />
          <StatChip label="Pivot" value={fmt(patternSummary.pivotPrice)} tone="text-amber-200" />
          <StatChip label="Invalid" value={fmt(patternSummary.invalidationPrice)} tone="text-rose-200" />
        </div>
      ) : null}
      <div
        ref={chartEl}
        className={cn(
          'w-full overflow-hidden rounded-md border border-slate-800 bg-slate-950',
          isFullscreen ? 'h-[calc(100vh-210px)] min-h-[520px]' : 'h-[390px]',
        )}
      />
    </div>
  );
}

function normalizeDate(value?: string | null): string | null {
  if (!value) return null;
  const date = value.slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : null;
}

function nearestTime(date: string | null, candles: CandlestickData[], timeSet: Set<string>): Time | null {
  if (!date || candles.length === 0) return null;
  if (timeSet.has(date)) return date as Time;
  const target = new Date(date).getTime();
  let bestTime: Time | null = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  candles.forEach((candle) => {
    const distance = Math.abs(new Date(String(candle.time)).getTime() - target);
    if (distance < bestDistance) {
      bestTime = candle.time;
      bestDistance = distance;
    }
  });
  return bestTime;
}

function buildPatternOverlay(
  pattern: ChartPatternOverlay | null | undefined,
  candles: CandlestickData[],
  timeSet: Set<string>,
): {
  summary: Required<Pick<ChartPatternOverlay, 'family'>> & Omit<ChartPatternOverlay, 'family'>;
  markers: SeriesMarker<Time>[];
  lines: Parameters<ISeriesApi<'Candlestick'>['createPriceLine']>[0][];
} | null {
  if (!pattern?.family || pattern.family === 'N/A' || candles.length === 0) return null;

  const last = candles[candles.length - 1];
  const signalTime = nearestTime(normalizeDate(pattern.signalDate ?? pattern.endDate), candles, timeSet) ?? last.time;
  const startTime = nearestTime(normalizeDate(pattern.startDate), candles, timeSet);
  const endTime = nearestTime(normalizeDate(pattern.endDate), candles, timeSet);
  const pivotPrice = pattern.pivotPrice ?? pattern.breakoutLevel ?? null;
  const markers: SeriesMarker<Time>[] = [];

  if (startTime) {
    markers.push({ time: startTime, position: 'belowBar', color: '#60a5fa', shape: 'circle', text: 'Base start', size: 0.85 });
  }
  if (endTime && endTime !== startTime) {
    markers.push({ time: endTime, position: 'aboveBar', color: '#f59e0b', shape: 'square', text: 'Base end', size: 0.85 });
  }
  if (pivotPrice != null) {
    markers.push({
      time: signalTime,
      position: 'atPriceTop',
      price: pivotPrice,
      color: '#fb7185',
      shape: 'arrowUp',
      text: pattern.family,
      size: 1.15,
    });
  } else {
    markers.push({
      time: signalTime,
      position: 'aboveBar',
      color: '#fb7185',
      shape: 'arrowUp',
      text: pattern.family,
      size: 1.15,
    });
  }

  const lines: Parameters<ISeriesApi<'Candlestick'>['createPriceLine']>[0][] = [];
  if (pivotPrice != null) {
    lines.push({
      price: pivotPrice,
      color: '#f59e0b',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: 'Pivot',
    });
  }
  if (pattern.invalidationPrice != null) {
    lines.push({
      price: pattern.invalidationPrice,
      color: '#fb7185',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: 'Invalid',
    });
  }

  return {
    summary: {
      family: pattern.family,
      state: pattern.state ?? null,
      setupQuality: pattern.setupQuality ?? null,
      pivotPrice,
      breakoutLevel: pattern.breakoutLevel ?? null,
      invalidationPrice: pattern.invalidationPrice ?? null,
      signalDate: pattern.signalDate ?? null,
      startDate: pattern.startDate ?? null,
      endDate: pattern.endDate ?? null,
    },
    markers,
    lines,
  };
}

function LegendItem({ label, value, tone = 'text-slate-300' }: { label: string; value: string; tone?: string }) {
  return (
    <span className="font-mono">
      <span className="text-slate-500">{label}</span>{' '}
      <span className={tone}>{value}</span>
    </span>
  );
}

function StatChip({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <span className="rounded-md border border-slate-800 bg-slate-950/45 px-2 py-1 font-mono">
      <span className="text-slate-500">{label}</span>{' '}
      <span className={tone}>{value}</span>
    </span>
  );
}
