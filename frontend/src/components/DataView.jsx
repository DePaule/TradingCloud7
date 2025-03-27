import React, { useState } from 'react';
import ChartComponent from './ChartComponent';
import TableComponent from './TableComponent';

/**
 * Gekürzte, aber umfangreiche Liste von Dukascopy-Instrumenten.
 * Du kannst hier weitere hinzufügen (z.B. alle Stocks/ETFs).
 */
const DUKASCOPY_ASSETS = [
  // Forex (große Auswahl)
  'AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD','CADCHF','CADJPY','CHFJPY','EURAUD','EURCAD',
  'EURCHF','EURCZK','EURDKK','EURGBP','EURHKD','EURHUF','EURJPY','EURNOK','EURNZD','EURPLN',
  'EURRUB','EURSEK','EURSGD','EURTRY','EURUSD','GBPAUD','GBPCAD','GBPCHF','GBPDKK','GBPHKD',
  'GBPJPY','GBPNOK','GBPNZD','GBPPLN','GBPSGD','GBPUSD','NZDCAD','NZDCHF','NZDJPY','NZDUSD',
  'SGDJPY','USDCAD','USDCHF','USDCNH','USDDKK','USDHKD','USDHUF','USDILS','USDJPY','USDMXN',
  'USDNOK','USDPLN','USDRUB','USDSEK','USDSGD','USDTRY','USDZAR','ZARJPY',

  // Metals
  'XAGUSD','XAUUSD','XPTUSD','XPDUSD',

  // Indizes (CFDs)
  'DEU.IDX/EUR','FRA.IDX/EUR','GBR.IDX/GBP','HKG.IDX/HKD','JPN.IDX/JPY','USA30.IDX/USD',
  'USA100.IDX/USD','USA500.IDX/USD','EUSTX50.IDX/EUR','CHE.IDX/CHF','AUS.IDX/AUD',
  'ESP.IDX/EUR','POL.IDX/PLN','UK100.IDX/GBP','NAS.IDX/USD','VOL.IDX/USD','ITA.IDX/EUR',
  'SUI.IDX/CHF','DEN.IDX/DKK','SWE.IDX/SEK',

  // Cryptos (CFDs)
  'BTC/USD','ETH/USD','LTC/USD','BCH/USD','EOS/USD','XRP/USD','DSH/USD','XLM/USD','ADA/USD','XMR/USD',

  // Beispielhaft ein paar Aktien (CFDs) - hier nur wenige
  'AAPL.US/USD','TSLA.US/USD','AMZN.US/USD','GOOG.US/USD','NFLX.US/USD',
];

/** 
 * Alle gewünschten Timeframes 
 */
const TIMEFRAMES = [
  'M1','M2','M3','M4','M5','M6','M7','M10','M12',
  'M15','M20','M30','H1','H2','H4','D1'
];

const DataView = () => {
  const [asset, setAsset] = useState('EURUSD');
  const [timeframe, setTimeframe] = useState('M10');
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');
  const [viewType, setViewType] = useState('table'); // "chart" oder "table"
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
        end: new Date(end).toISOString()
      });
      // Hier dein Endpoint anpassen, falls du ein anderes Backend hast
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
      <form onSubmit={fetchData} style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'left' }}>
        <label>Asset</label>
        <select value={asset} onChange={(e) => setAsset(e.target.value)}>
          {DUKASCOPY_ASSETS.map(sym => (
            <option key={sym} value={sym}>{sym}</option>
          ))}
        </select>

        <label>Timeframe</label>
        <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)}>
          {TIMEFRAMES.map(tf => (
            <option key={tf} value={tf}>{tf}</option>
          ))}
        </select>

        <label>Von</label>
        <input
          type="datetime-local"
          value={start}
          onChange={(e) => setStart(e.target.value)}
        />

        <label>Bis</label>
        <input
          type="datetime-local"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
        />

        <label>Ansicht</label>
        <select value={viewType} onChange={(e) => setViewType(e.target.value)}>
          <option value="chart">Chart</option>
          <option value="table">Tabelle</option>
        </select>

        <button type="submit">Daten abrufen</button>
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
