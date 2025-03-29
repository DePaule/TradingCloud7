import React, { useState, useEffect } from 'react';

interface Candle {
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface Instrument {
  instrument_id: string;
  instrument_name: string;
  description: string;
}

const App: React.FC = () => {
  // Calculate default dates: "to" is today, "from" is 30 days ago
  const today = new Date().toISOString().substring(0, 10);
  const thirtyDaysAgo = new Date(new Date().setDate(new Date().getDate() - 30))
    .toISOString()
    .substring(0, 10);

  // State for form fields
  const [groups, setGroups] = useState<string[]>([]);
  const [selectedGroup, setSelectedGroup] = useState<string>('fx_majors');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<string>('eurusd'); // default prefilled asset
  const [timeframe, setTimeframe] = useState<string>('M5'); // default prefilled timeframe
  const [startDate, setStartDate] = useState<string>(thirtyDaysAgo);
  const [endDate, setEndDate] = useState<string>(today);
  const [candles, setCandles] = useState<Candle[]>([]);
  const [error, setError] = useState<string>('');

  // Fetch available groups from backend on mount
  useEffect(() => {
    fetch('/api/instrument-groups')
      .then((res) => res.json())
      .then((data) => {
        if (data.groups && data.groups.length > 0) {
          setGroups(data.groups);
          if (!data.groups.includes('fx_majors')) {
            setSelectedGroup(data.groups[0]);
          }
        }
      })
      .catch((err) => {
        console.error('Error fetching groups:', err);
      });
  }, []);

  // When the selected group changes, fetch instruments for that group
  useEffect(() => {
    fetch(`/api/instruments?group=${selectedGroup}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.instruments) {
          setInstruments(data.instruments);
          // Set default asset if available
          if (data.instruments.length > 0) {
            setSelectedAsset(data.instruments[0].instrument_id);
          }
        }
      })
      .catch((err) => {
        console.error('Error fetching instruments:', err);
      });
  }, [selectedGroup]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setCandles([]);
    // Build query parameters; backend will adjust times to 00:00 and 23:59
    const params = new URLSearchParams({
      asset: selectedAsset,
      resolution: timeframe,
      start: startDate,
      end: endDate,
    });
    try {
      const response = await fetch(`/api/candles?${params.toString()}`);
      if (!response.ok) {
        // Read error text from response
        const errorText = await response.text();
        throw new Error(errorText);
      }
      const data = await response.json();
      setCandles(data.candles);
    } catch (err: any) {
      // If the error message suggests the table does not exist, set candles to empty
      if (err.message.includes("does not exist") || err.message.includes("relation")) {
        setCandles([]);
        setError("No data available for the selected asset (table not found).");
      } else {
        setError(err.message);
      }
    }
  };

  // Inline styles for a slim, horizontal layout
  const formContainerStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    flexWrap: 'wrap',
    gap: '0.5rem',
    fontSize: '12px',
    marginBottom: '1rem',
  };

  const inputStyle: React.CSSProperties = {
    padding: '0.25rem 0.5rem',
    fontSize: '12px',
    minWidth: '100px',
  };

  const labelStyle: React.CSSProperties = {
    display: 'flex',
    flexDirection: 'column',
    fontSize: '12px',
  };

  return (
    <div style={{ padding: '1rem', fontFamily: 'Arial, sans-serif' }}>
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
        {/* Timeframe input as free text */}
        <div style={labelStyle}>
          <label>
            Timeframe
            <input
              type="text"
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value)}
              placeholder="e.g., S20, M4, H1"
              required
              style={inputStyle}
            />
          </label>
        </div>
        {/* Date range selection */}
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
        <div style={{ color: 'red', fontSize: '12px', marginBottom: '1rem' }}>{error}</div>
      )}
      {candles.length > 0 && (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
          <thead>
            <tr>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>Time</th>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>Open</th>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>High</th>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>Low</th>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>Close</th>
              <th style={{ border: '1px solid #ccc', padding: '0.3rem' }}>Volume</th>
            </tr>
          </thead>
          <tbody>
            {candles.map((candle, index) => (
              <tr key={index}>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.bucket}</td>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.open}</td>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.high}</td>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.low}</td>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.close}</td>
                <td style={{ border: '1px solid #ccc', padding: '0.3rem' }}>{candle.volume}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
};

export default App;
