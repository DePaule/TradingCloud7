import React, { useState, FormEvent } from 'react';

interface CandleData {
  candles: any[];
}

const App: React.FC = () => {
  const [asset, setAsset] = useState<string>("EURUSD");
  const [resolution, setResolution] = useState<string>("M10");
  const [start, setStart] = useState<string>("");
  const [end, setEnd] = useState<string>("");
  const [data, setData] = useState<CandleData | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const handleFetch = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);

    const queryParams = new URLSearchParams({
      asset,
      resolution,
      start,
      end,
    });

    try {
      const response = await fetch(`http://localhost:8000/api/candles?${queryParams.toString()}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const result: CandleData = await response.json();
      setData(result);
    } catch (err: any) {
      setError(err.toString());
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: '20px' }}>
      <h1>Candlestick Data Fetcher</h1>
      <form onSubmit={handleFetch}>
        <div>
          <label>Asset: </label>
          <input
            type="text"
            value={asset}
            onChange={(e) => setAsset(e.target.value)}
          />
        </div>
        <div>
          <label>Resolution (z.B. M10): </label>
          <input
            type="text"
            value={resolution}
            onChange={(e) => setResolution(e.target.value)}
          />
        </div>
        <div>
          <label>Von (ISO-Datum): </label>
          <input
            type="text"
            value={start}
            onChange={(e) => setStart(e.target.value)}
            placeholder="YYYY-MM-DDTHH:mm:ssZ"
          />
        </div>
        <div>
          <label>Bis (ISO-Datum): </label>
          <input
            type="text"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
            placeholder="YYYY-MM-DDTHH:mm:ssZ"
          />
        </div>
        <button type="submit">Daten abrufen</button>
      </form>

      {loading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}
      {data && (
        <div>
          <h2>Abgerufene Candle-Daten</h2>
          <pre>{JSON.stringify(data, null, 2)}</pre>
        </div>
      )}
    </div>
  );
};

export default App;
