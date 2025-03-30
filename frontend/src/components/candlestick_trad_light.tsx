import React, { useEffect, useRef } from 'react';
import { createChart, IChartApi, CrosshairMode } from 'lightweight-charts';

export interface Candle {
  time: string; // format: 'YYYY-MM-DD' (or Unix timestamp)
  open: number;
  high: number;
  low: number;
  close: number;
  // volume is optional
}

interface CandlestickTradLightProps {
  candles: Candle[];
}

const CandlestickTradLight: React.FC<CandlestickTradLightProps> = ({ candles }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (chartContainerRef.current) {
      // Create the chart with a variety of interactive features enabled
      const chart = createChart(chartContainerRef.current, {
        width: chartContainerRef.current.clientWidth,
        height: 400,
        layout: {
          textColor: '#000',
        },
        grid: {
          vertLines: { color: '#eee' },
          horzLines: { color: '#eee' },
        },
        crosshair: {
          mode: CrosshairMode.Normal,
        },
        rightPriceScale: {
          borderColor: '#ccc',
        },
        timeScale: {
          borderColor: '#ccc',
          timeVisible: true,
          secondsVisible: false,
        },
      });
      chartRef.current = chart;

      // Add candlestick series with custom colors
      const candleSeries = chart.addCandlestickSeries({
        upColor: '#00da3c',
        downColor: '#ec0000',
        borderDownColor: '#8A0000',
        borderUpColor: '#008F28',
        wickDownColor: '#8A0000',
        wickUpColor: '#008F28',
      });

      // Set the data (if available)
      candleSeries.setData(candles);

      // Enable additional interactive features by exposing built-in options:
      // (The chart already supports panning, zooming, and crosshair by default.)
      // You can customize the tooltip via the chart API if needed.

      // Handle resizing
      const handleResize = () => {
        if (chartContainerRef.current) {
          chart.applyOptions({ width: chartContainerRef.current.clientWidth });
        }
      };
      window.addEventListener('resize', handleResize);

      return () => {
        window.removeEventListener('resize', handleResize);
        chart.remove();
      };
    }
  }, [candles]);

  return <div ref={chartContainerRef} style={{ border: '1px solid #ccc' }} />;
};

export default CandlestickTradLight;
