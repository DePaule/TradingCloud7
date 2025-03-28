import React, { useEffect, useRef } from 'react';
import { createChart } from 'lightweight-charts';

const ChartComponent = ({ data }) => {
  const chartContainerRef = useRef();

  useEffect(() => {
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

    // Konvertiere Backend-Daten in das von lightweight-charts erwartete Format:
    const chartData = data.map(candle => ({
      time: Math.floor(new Date(candle.bucket).getTime() / 1000),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close
    }));

    candleSeries.setData(chartData);

    // Füge eine konstante Preislinie bei 1 hinzu:
    candleSeries.createPriceLine({
      price: 1,
      color: 'red',
      lineWidth: 2,
      lineStyle: 2,
      axisLabelVisible: true,
      title: '1'
    });

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
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
