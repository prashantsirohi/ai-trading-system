/**
 * Professional candlestick chart for the canonical stock page.
 *
 * Phase 1: OHLC candles, volume histogram, SMA 50/200, responsive sizing,
 * timeframe controls, and a compact legend. Pattern/pivot markers can layer
 * on top of this component in the next phase.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import {
  ColorType,
  CrosshairMode,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type Time,
} from 'lightweight-charts';
import type { StockOhlcv } from '@/lib/api/stocks';
import { deriveMAs } from '@/lib/symbol/derive';
import { cn } from '@/lib/utils/cn';

type Period = '3M' | '6M' | '1Y' | 'All';

const PERIODS: Period[] = ['3M', '6M', '1Y', 'All'];
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

interface Props {
  data: StockOhlcv | null | undefined;
  isLoading: boolean;
  breakoutDate?: string | null;
}

export default function SymbolChart({ data, isLoading }: Props) {
  const chartEl = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const ma50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ma200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  const [period, setPeriod] = useState<Period>('1Y');
  const [legend, setLegend] = useState<{
    date: string;
    open: number | null;
    high: number | null;
    low: number | null;
    close: number | null;
    volume: number | null;
  } | null>(null);

  const prepared = useMemo(() => {
    if (!data?.available || data.candles.length === 0) {
      return { candles: [], volumes: [], ma50: [], ma200: [] };
    }

    const limit = PERIOD_LIMIT[period];
    const sliced = Number.isFinite(limit) ? data.candles.slice(-limit) : data.candles;
    const { ma50, ma200 } = deriveMAs(sliced);

    const candles: CandlestickData[] = [];
    const volumes: HistogramData[] = [];
    const ma50Line: LineData[] = [];
    const ma200Line: LineData[] = [];

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
      if (ma50[index] != null) ma50Line.push({ time, value: ma50[index] as number });
      if (ma200[index] != null) ma200Line.push({ time, value: ma200[index] as number });
    });

    return { candles, volumes, ma50: ma50Line, ma200: ma200Line };
  }, [data, period]);

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

    chartRef.current = chart;
    candleSeriesRef.current = candles;
    volumeSeriesRef.current = volume;
    ma50SeriesRef.current = ma50;
    ma200SeriesRef.current = ma200;

    chart.subscribeCrosshairMove((param) => {
      const candle = param.seriesData.get(candles) as CandlestickData | undefined;
      const vol = param.seriesData.get(volume) as HistogramData | undefined;
      if (!candle) {
        const last = prepared.candles[prepared.candles.length - 1];
        const lastVol = prepared.volumes[prepared.volumes.length - 1];
        setLegend(last ? {
          date: String(last.time),
          open: last.open,
          high: last.high,
          low: last.low,
          close: last.close,
          volume: lastVol?.value ?? null,
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
      });
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ma50SeriesRef.current = null;
      ma200SeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    candleSeriesRef.current?.setData(prepared.candles);
    volumeSeriesRef.current?.setData(prepared.volumes);
    ma50SeriesRef.current?.setData(prepared.ma50);
    ma200SeriesRef.current?.setData(prepared.ma200);
    chartRef.current?.timeScale().fitContent();

    const last = prepared.candles[prepared.candles.length - 1];
    const lastVol = prepared.volumes[prepared.volumes.length - 1];
    setLegend(last ? {
      date: String(last.time),
      open: last.open,
      high: last.high,
      low: last.low,
      close: last.close,
      volume: lastVol?.value ?? null,
    } : null);
  }, [prepared]);

  if (isLoading) {
    return <div className="flex h-80 items-center justify-center text-sm text-slate-500">Loading chart...</div>;
  }
  if (!data?.available || prepared.candles.length === 0) {
    return <div className="flex h-80 items-center justify-center text-sm text-slate-500">No price history available.</div>;
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs">
          <span className="font-semibold text-slate-200">Candles</span>
          <span className="text-slate-500">{legend?.date ?? '-'}</span>
          <LegendItem label="O" value={fmt(legend?.open)} />
          <LegendItem label="H" value={fmt(legend?.high)} tone="text-emerald-300" />
          <LegendItem label="L" value={fmt(legend?.low)} tone="text-rose-300" />
          <LegendItem label="C" value={fmt(legend?.close)} />
          <LegendItem label="Vol" value={fmtVolume(legend?.volume)} />
          <span className="text-amber-300">SMA50</span>
          <span className="text-blue-300">SMA200</span>
        </div>
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
      </div>
      <div ref={chartEl} className="h-[390px] w-full overflow-hidden rounded-md border border-slate-800 bg-slate-950" />
    </div>
  );
}

function LegendItem({ label, value, tone = 'text-slate-300' }: { label: string; value: string; tone?: string }) {
  return (
    <span className="font-mono">
      <span className="text-slate-500">{label}</span>{' '}
      <span className={tone}>{value}</span>
    </span>
  );
}
