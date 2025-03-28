import React, { useEffect, useRef } from 'react';
import { createChart } from 'lightweight-charts';

interface Candle {
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
}

interface ChartProps {
  data: Candle[];
}

const ChartComponent: React.FC<ChartProps> = ({ data }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 400,
      layout: {
        backgroundColor: '#ffffff',
        textColor: '#333'
      },
      grid: {
        vertLines: { color: '#eee' },
        horzLines: { color: '#eee' }
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false
      }
    });

    const candleSeries = chart.addCandlestickSeries();
    const chartData = data.map(candle => ({
      time: Math.floor(new Date(candle.bucket).getTime() / 1000),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close
    }));

    candleSeries.setData(chartData);

    candleSeries.createPriceLine({
      price: 1,
      color: 'red',
      lineWidth: 2,
      lineStyle: 2,
      axisLabelVisible: true,
      title: '1'
    });

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
  }, [data]);

  return <div ref={chartContainerRef} style={{ position: 'relative', width: '100%' }} />;
};

export default ChartComponent;
