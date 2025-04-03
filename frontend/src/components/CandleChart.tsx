import React, { useEffect, useRef } from 'react';
import {
  createChart,
  IChartApi,
  ChartOptions,
  CandlestickSeries,
  CandlestickSeriesPartialOptions,
  ISeriesApi,
  DeepPartial
} from 'lightweight-charts';

export interface Candle {
  // ISO string mit Datum und Uhrzeit (z. B. "2023-04-11T09:43:00Z")
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  signal?: number; // 1 für Buy, -1 für Sell (optional)
}

interface CandleChartProps {
  candles: Candle[];
}

const CandleChart: React.FC<CandleChartProps> = ({ candles }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);

  useEffect(() => {
    if (chartContainerRef.current) {
      // Create the chart with specified options
      chartRef.current = createChart(chartContainerRef.current, {
        width: chartContainerRef.current.clientWidth,
        height: 400,
        layout: {
          background: { color: '#ffffff' },
          textColor: '#000'
        },
        grid: {
          vertLines: { color: '#eee' },
          horzLines: { color: '#eee' }
        },
        crosshair: { mode: 1 },
        rightPriceScale: { borderColor: '#ccc' },
        timeScale: {
          borderColor: '#ccc',
          timeVisible: true,
          secondsVisible: true
        }
      } as DeepPartial<ChartOptions>);

      // Add the candlestick series
      seriesRef.current = chartRef.current.addSeries(CandlestickSeries, {
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderDownColor: '#ef5350',
        borderUpColor: '#26a69a',
        wickDownColor: '#ef5350',
        wickUpColor: '#26a69a'
      } as CandlestickSeriesPartialOptions);
    }

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };

    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
      chartRef.current?.remove();
    };
  }, []);

  // Update series data when candles change
  useEffect(() => {
    if (seriesRef.current && candles.length > 0) {
      const data = candles.map(candle => ({
        // Convert the ISO string (including time) to a Unix timestamp in seconds
        time: Math.floor(Date.parse(candle.bucket) / 1000),
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close
      }));
      seriesRef.current.setData(data);
    }
  }, [candles]);

  // Set markers for buy/sell signals, if available
  useEffect(() => {
    if (seriesRef.current) {
      const markers = candles
        .filter(candle => candle.signal === 1 || candle.signal === -1)
        .map(candle => {
          const timeStamp = Math.floor(Date.parse(candle.bucket) / 1000);
          return candle.signal === 1
            ? { time: timeStamp, position: 'aboveBar' as const, color: 'green', shape: 'arrowUp' as const, text: 'Buy' }
            : { time: timeStamp, position: 'belowBar' as const, color: 'red', shape: 'arrowDown' as const, text: 'Sell' };
        });
      // Check if setMarkers is available before calling it
      const series: any = seriesRef.current;
      if (typeof series.setMarkers === 'function') {
        series.setMarkers(markers);
      } else {
        console.warn('setMarkers is not available on the current series.');
      }
    }
  }, [candles]);

  return <div ref={chartContainerRef} />;
};

export default CandleChart;
