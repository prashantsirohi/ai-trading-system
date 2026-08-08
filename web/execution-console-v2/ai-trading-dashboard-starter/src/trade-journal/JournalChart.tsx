import { useEffect, useRef } from 'react';
import {
  CandlestickSeries,
  ColorType,
  createChart,
  createSeriesMarkers,
  type CandlestickData,
  type SeriesMarker,
  type Time,
} from 'lightweight-charts';

export interface JournalBar {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  symbol: string;
}

export interface FillMarker {
  fill_id: string;
  time: string;
  side: string;
  price: string;
  quantity: string;
  link_type: string;
}

export function JournalChart({ bars, markers }: { bars: JournalBar[]; markers: FillMarker[] }) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!host.current || bars.length === 0) return;
    const chart = createChart(host.current, {
      height: 420,
      layout: { background: { type: ColorType.Solid, color: '#0e1b26' }, textColor: '#c7d8df' },
      grid: { vertLines: { color: '#1e3340' }, horzLines: { color: '#1e3340' } },
      timeScale: { borderColor: '#29404f' },
      rightPriceScale: { borderColor: '#29404f' },
    });
    const candles = chart.addSeries(CandlestickSeries, {
      upColor: '#65d6c3', downColor: '#ff8d8d', borderVisible: false,
      wickUpColor: '#65d6c3', wickDownColor: '#ff8d8d',
    });
    candles.setData(bars.map((bar): CandlestickData => ({
      time: bar.time.slice(0, 10) as Time,
      open: Number(bar.open), high: Number(bar.high), low: Number(bar.low), close: Number(bar.close),
    })));
    const visibleTimes = new Set(bars.map((bar) => bar.time.slice(0, 10)));
    const chartMarkers = markers
      .filter((marker) => visibleTimes.has(marker.time.slice(0, 10)))
      .map((marker): SeriesMarker<Time> => ({
        time: marker.time.slice(0, 10) as Time,
        position: marker.side.toLowerCase() === 'buy' ? 'belowBar' : 'aboveBar',
        color: marker.side.toLowerCase() === 'buy' ? '#65d6c3' : '#ff8d8d',
        shape: marker.side.toLowerCase() === 'buy' ? 'arrowUp' : 'arrowDown',
        text: `${marker.side.toUpperCase()} ${marker.quantity} @ ${marker.price}`,
      }));
    createSeriesMarkers(candles, chartMarkers);
    chart.timeScale().fitContent();
    const observer = new ResizeObserver(([entry]) => chart.applyOptions({ width: entry.contentRect.width }));
    observer.observe(host.current);
    return () => { observer.disconnect(); chart.remove(); };
  }, [bars, markers]);
  if (!bars.length) return <div className="state-card" role="status"><strong>Chart unavailable</strong><p>No trusted OHLCV bars cover this episode.</p></div>;
  return <div ref={host} className="journal-chart" role="img" aria-label="Episode candlestick chart with buy and sell fill markers" />;
}
