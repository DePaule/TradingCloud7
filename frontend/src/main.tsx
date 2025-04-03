import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom/client';
import CandleChart, { Candle } from './components/CandleChart';
import CandleDataTable from './components/CandleDataTable';
import './index.css';

// Define interface for instrument metadata
interface Instrument {
  instrument_id: string;
  instrument_name: string;
  description: string;
}

// Main App component (merged version of App.tsx and main.tsx)
const App: React.FC = () => {
  // Set default dates: today and 30 days ago (ISO string format)
  const today = new Date().toISOString().substring(0, 10);
  const thirtyDaysAgo = new Date(new Date().setDate(new Date().getDate() - 30))
    .toISOString()
    .substring(0, 10);

  // Define state variables for form inputs and fetched data
  const [groups, setGroups] = useState<string[]>([]);
  const [selectedGroup, setSelectedGroup] = useState<string>('fx_majors');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<string>('eurusd');
  const [timeframe, setTimeframe] = useState<string>('M5');
  const [startDate, setStartDate] = useState<string>(thirtyDaysAgo);
  const [endDate, setEndDate] = useState<string>(today);
  const [candles, setCandles] = useState<Candle[]>([]);
  const [error, setError] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);

  // Fetch available instrument groups on component mount
  useEffect(() => {
    console.log('Fetching instrument groups...');
    fetch('/api/instrument-groups')
      .then((res) => res.json())
      .then((data) => {
        if (data.groups && data.groups.length > 0) {
          setGroups(data.groups);
          // Use 'fx_majors' if available; otherwise, take the first group
          const defaultGroup = data.groups.includes('fx_majors') ? 'fx_majors' : data.groups[0];
          setSelectedGroup(defaultGroup);
        }
      })
      .catch((err) => {
        console.error('Error fetching instrument groups:', err);
        setError('Error fetching instrument groups.');
      });
  }, []);

  // Fetch instruments whenever the selected group changes
  useEffect(() => {
    if (!selectedGroup) return;
    console.log(`Fetching instruments for group: ${selectedGroup}`);
    fetch(`/api/instruments?group=${selectedGroup}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.instruments) {
          setInstruments(data.instruments);
          // Set default asset based on first instrument available
          if (data.instruments.length > 0) {
            setSelectedAsset(data.instruments[0].instrument_id);
          }
        }
      })
      .catch((err) => {
        console.error('Error fetching instruments:', err);
        setError('Error fetching instruments.');
      });
  }, [selectedGroup]);

  // Handle form submission to fetch candle data
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    setCandles([]);
    const params = new URLSearchParams({
      asset: selectedAsset,
      resolution: timeframe,
      start: startDate,
      end: endDate,
    });
    try {
      console.log('Fetching candle data with params:', params.toString());
      const response = await fetch(`/api/candles?${params.toString()}`);
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText);
      }
      const data = await response.json();
      console.log('Candle data received:', data);
      setCandles(data.candles);
    } catch (err: any) {
      console.error('Error fetching candle data:', err);
      if (
        err.message.includes('does not exist') ||
        err.message.includes('relation')
      ) {
        setError('No data available for the selected asset (table not found).');
      } else {
        setError(err.message);
      }
    } finally {
      setLoading(false);
    }
  };

  // Callback to update signal value in candle data
  const handleSignalChange = (bucket: string, newSignal: number | null) => {
    setCandles((prevCandles) =>
      prevCandles.map((candle) =>
        candle.bucket === bucket
          ? { ...candle, signal: newSignal !== null ? newSignal : undefined }
          : candle
      )
    );
  };

  // Inline styles for form layout
  const formContainerStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    flexWrap: 'wrap',
    gap: '0.5rem',
    fontSize: '12px',
    marginBottom: '1rem',
  };

  const labelStyle: React.CSSProperties = {
    display: 'flex',
    flexDirection: 'column',
    fontSize: '12px',
  };

  const inputStyle: React.CSSProperties = {
    padding: '0.25rem 0.5rem',
    fontSize: '12px',
    minWidth: '100px',
  };

  return (
    <div style={{ padding: '1rem', fontFamily: 'Arial, sans-serif' }}>

      {/* Debugger statement to force a break if needed */}
      {/* debugger; */}
      <form onSubmit={handleSubmit} style={formContainerStyle}>
        {/* Group selection */}
        <div style={labelStyle}>
          <label>
            Group
            <select
              value={selectedGroup}
              onChange={(e) => setSelectedGroup(e.target.value)}
              style={inputStyle}
            >
              {groups.map((group) => (
                <option key={group} value={group}>
                  {group}
                </option>
              ))}
            </select>
          </label>
        </div>
        {/* Asset selection */}
        <div style={labelStyle}>
          <label>
            Asset
            <select
              value={selectedAsset}
              onChange={(e) => setSelectedAsset(e.target.value)}
              style={inputStyle}
            >
              {instruments.map((inst) => (
                <option key={inst.instrument_id} value={inst.instrument_id}>
                  {inst.instrument_name}
                </option>
              ))}
            </select>
          </label>
        </div>
        {/* Timeframe input */}
        <div style={labelStyle}>
          <label>
            Timeframe
            <input
              type="text"
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value)}
              placeholder="e.g., M5, H1, D1"
              required
              style={inputStyle}
            />
          </label>
        </div>
        {/* Date range inputs */}
        <div style={labelStyle}>
          <label>
            From
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              required
              style={inputStyle}
            />
          </label>
        </div>
        <div style={labelStyle}>
          <label>
            To
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              required
              style={inputStyle}
            />
          </label>
        </div>
        <div>
          <button type="submit" style={{ padding: '0.4rem 0.8rem', fontSize: '12px' }}>
            Fetch Data
          </button>
        </div>
      </form>
      {error && (
        <div style={{ color: 'red', fontSize: '12px', marginBottom: '1rem' }}>
          {error}
        </div>
      )}
      {loading && <div style={{ fontSize: '12px' }}>Loading data...</div>}
      {candles.length > 0 && (
        <div>
          <CandleChart candles={candles} />
          <CandleDataTable candles={candles} onSignalChange={handleSignalChange} />
        </div>
      )}
    </div>
  );
};

// Render the App component into the root element
const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement);
root.render(<App />);
