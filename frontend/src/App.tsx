import React, { useState, FormEvent } from 'react';
import axios from 'axios';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  TimeScale
} from 'chart.js';
import 'chartjs-adapter-date-fns';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend, TimeScale);

interface Candle {
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

const App: React.FC = () => {
  const [asset, setAsset] = useState<string>('GBPUSD');
  const [resolution, setResolution] = useState<string>('M10');
  const [start, setStart] = useState<string>('2025-03-24T00:00');
  const [end, setEnd] = useState<string>('2025-03-25T23:59');
  const [candles, setCandles] = useState<Candle[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    try {
      const startISO = new Date(start).toISOString();
      const endISO = new Date(end).toISOString();
      const response = await axios.get('/api/candles', {
        params: { asset, resolution, start: startISO, end: endISO }
      });
      setCandles(response.data.candles);
    } catch (err: any) {
      console.error(err);
      setError('Error fetching data');
    }
    setLoading(false);
  };

  const chartData = {
    labels: candles.map(c => new Date(c.bucket)),
    datasets: [
      {
        label: 'Open',
        data: candles.map(c => c.open),
        borderColor: 'blue',
        fill: false
      },
      {
        label: 'High',
        data: candles.map(c => c.high),
        borderColor: 'green',
        fill: false
      },
      {
        label: 'Low',
        data: candles.map(c => c.low),
        borderColor: 'red',
        fill: false
      },
      {
        label: 'Close',
        data: candles.map(c => c.close),
        borderColor: 'orange',
        fill: false
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    scales: {
      x: {
        type: 'time',
        time: { tooltipFormat: 'PPpp' },
        title: { display: true, text: 'Time' }
      },
      y: {
        title: { display: true, text: 'Price' }
      }
    }
  };

  return (
    <div style={{ margin: '20px' }}>
      <h1>TradingCloud Data Viewer</h1>
      <form onSubmit={handleSubmit}>
        <div>
          <label>Asset: </label>
          <input type="text" value={asset} onChange={(e) => setAsset(e.target.value)} required />
        </div>
        <div>
          <label>Resolution (e.g., M10): </label>
          <input type="text" value={resolution} onChange={(e) => setResolution(e.target.value)} required />
        </div>
        <div>
          <label>Start Date: </label>
          <input type="datetime-local" value={start} onChange={(e) => setStart(e.target.value)} required />
        </div>
        <div>
          <label>End Date: </label>
          <input type="datetime-local" value={end} onChange={(e) => setEnd(e.target.value)} required />
        </div>
        <button type="submit">Load Data</button>
      </form>
      {loading && <p>Loading data...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {candles.length > 0 && (
        <>
          <h2>Data Table</h2>
          <table style={{ borderCollapse: 'collapse', width: '100%' }}>
            <thead>
              <tr>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>Time Bucket</th>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>Open</th>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>High</th>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>Low</th>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>Close</th>
                <th style={{ border: '1px solid #ddd', padding: '8px' }}>Volume</th>
              </tr>
            </thead>
            <tbody>
              {candles.map((candle, index) => (
                <tr key={index}>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>
                    {new Date(candle.bucket).toLocaleString()}
                  </td>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>{candle.open}</td>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>{candle.high}</td>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>{candle.low}</td>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>{candle.close}</td>
                  <td style={{ border: '1px solid #ddd', padding: '8px' }}>{candle.volume}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h2>Candlestick Chart</h2>
          <Line data={chartData} options={chartOptions} />
        </>
      )}
    </div>
  );
};

export default App;
