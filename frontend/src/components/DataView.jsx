import React, { useState } from 'react';
import ChartComponent from './ChartComponent';
import TableComponent from './TableComponent';

// Hier alle Dukascopy-Instrumente einfügen (dies ist ein Beispiel – ergänze bei Bedarf weitere):
const DUKASCOPY_ASSETS = [
  'AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD',
  'CADCHF','CADJPY','CHFJPY','EURAUD','EURCAD',
  'EURCHF','EURGBP','EURJPY','EURNZD','EURUSD',
  'GBPAUD','GBPCAD','GBPCHF','GBPJPY','GBPNZD',
  'GBPUSD','NZDCAD','NZDCHF','NZDJPY','NZDUSD',
  'USDCAD','USDCHF','USDJPY'
  // Weitere Instrumente: siehe https://www.dukascopy-node.app/instruments
];

// Erweitertes Timeframe-Array (M2, M3, M4, M6, M7, M10, M12, M15, M20, M30, H1, H2, H4, D1)
const TIMEFRAMES = [
  'M2','M3','M4','M6','M7','M10','M12','M15','M20','M30','H1','H2','H4','D1'
];

// Hilfsfunktion: Formatiert ein Datum im "datetime-local"-Format (YYYY-MM-DDTHH:MM)
const toDateTimeLocal = (date) => {
  const pad = (n) => String(n).padStart(2, '0');
  const YYYY = date.getFullYear();
  const MM = pad(date.getMonth() + 1);
  const DD = pad(date.getDate());
  const HH = pad(date.getHours());
  const mm = pad(date.getMinutes());
  return `${YYYY}-${MM}-${DD}T${HH}:${mm}`;
};

const now = new Date();
const defaultEnd = toDateTimeLocal(now);
const defaultStart = toDateTimeLocal(new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000));

const DataView = () => {
  const [asset, setAsset] = useState('EURUSD');
  const [timeframe, setTimeframe] = useState('M10');
  const [start, setStart] = useState(defaultStart);
  const [end, setEnd] = useState(defaultEnd);
  const [viewType, setViewType] = useState('chart'); // Standardmäßig Chart
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchData = async (e) => {
    e.preventDefault();
    setError('');
    setData([]);
    if (!asset || !start || !end) {
      setError('Bitte füllen Sie alle Felder aus.');
      return;
    }
    setLoading(true);
    try {
      const params = new URLSearchParams({
        asset,
        resolution: timeframe,
        start: new Date(start).toISOString(),
        end: new Date(end).toISOString(),
      });
      // Passe ggf. die Backend-Adresse an, falls dein API-Endpunkt nicht unter derselben Domain liegt!
      const response = await fetch(`/api/candles?${params.toString()}`);
      if (!response.ok) {
        throw new Error(`Fehler: ${response.statusText}`);
      }
      const result = await response.json();
      setData(result.candles);
    } catch (err) {
      setError(err.message);
    }
    setLoading(false);
  };

  return (
    <div>
      {/* Flaches, horizontales Formular – oben links */}
      <form
        onSubmit={fetchData}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          padding: '4px',
          margin: 0,
          flexWrap: 'nowrap'
        }}
      >
        <label style={{ margin: 0 }}>Asset</label>
        <select value={asset} onChange={(e) => setAsset(e.target.value)} style={{ margin: 0 }}>
          {DUKASCOPY_ASSETS.map((sym) => (
            <option key={sym} value={sym}>{sym}</option>
          ))}
        </select>

        <label style={{ margin: 0 }}>Timeframe</label>
        <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)} style={{ margin: 0 }}>
          {TIMEFRAMES.map((tf) => (
            <option key={tf} value={tf}>{tf}</option>
          ))}
        </select>

        <label style={{ margin: 0 }}>Von</label>
        <input
          type="datetime-local"
          value={start}
          onChange={(e) => setStart(e.target.value)}
          style={{ margin: 0 }}
        />

        <label style={{ margin: 0 }}>Bis</label>
        <input
          type="datetime-local"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
          style={{ margin: 0 }}
        />

        <label style={{ margin: 0 }}>Ansicht</label>
        <select value={viewType} onChange={(e) => setViewType(e.target.value)} style={{ margin: 0 }}>
          <option value="chart">Chart</option>
          <option value="table">Tabelle</option>
        </select>

        <button type="submit" style={{ margin: 0 }}>refresh</button>
      </form>

      {loading && <p>Lädt...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}

      {data.length > 0 && (
        viewType === 'chart'
          ? <ChartComponent data={data} />
          : <TableComponent data={data} />
      )}
    </div>
  );
};

export default DataView;
