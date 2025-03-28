import React, { useState, useEffect, useRef } from 'react';
import { createChart, CrosshairMode } from 'lightweight-charts';

/**
 * Collapsible Panel: Ein kleiner Helfer, um Bereiche ein-/auszublenden.
 */
function CollapsiblePanel({ title, children, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div style={{ marginTop: '1rem' }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          marginBottom: '0.5rem',
          cursor: 'pointer',
          padding: '4px 8px'
        }}
      >
        {open ? `Minimieren: ${title}` : `Vergrößern: ${title}`}
      </button>
      {open && <div>{children}</div>}
    </div>
  );
}

/**
 * CandleChart: Zeichnet das Candlestick-Chart (Hauptchart).
 * Zeigt beim MouseMove ein kleines Tooltip.
 */
function CandleChart({ candles }) {
  const chartContainerRef = useRef(null);
  const tooltipRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // Chart erstellen
    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 400,
      layout: {
        background: { type: 'Solid', color: '#ffffff' },
        textColor: '#333'
      },
      grid: {
        vertLines: { color: '#eee' },
        horzLines: { color: '#eee' }
      },
      crosshair: {
        mode: CrosshairMode.Normal
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false
      }
    });
    chartRef.current = chart;

    // Candlestick Serie
    const candleSeries = chart.addCandlestickSeries();
    seriesRef.current = candleSeries;

    // Konvertieren
    const chartData = candles.map(c => ({
      time: Math.floor(new Date(c.bucket).getTime() / 1000),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close
    }));
    candleSeries.setData(chartData);

    // PriceLine bei 1.0
    candleSeries.createPriceLine({
      price: 1.0,
      color: 'red',
      lineWidth: 2,
      lineStyle: 2,
      axisLabelVisible: true,
      title: '1.0'
    });

    // Tooltip-Element
    const tooltip = document.createElement('div');
    tooltip.style = `
      position: absolute;
      display: none;
      pointer-events: none;
      background: rgba(0, 0, 0, 0.7);
      color: #fff;
      padding: 5px;
      border-radius: 4px;
      font-size: 12px;
      z-index: 9999;
    `;
    chartContainerRef.current.appendChild(tooltip);
    tooltipRef.current = tooltip;

    // Crosshair Move -> Tooltip
    const handleCrosshairMove = (param) => {
      if (!param.point || !param.time || !param.seriesPrices.size) {
        tooltip.style.display = 'none';
        return;
      }
      const price = param.seriesPrices.get(candleSeries);
      if (!price) {
        tooltip.style.display = 'none';
        return;
      }
      tooltip.style.display = 'block';
      const { open, high, low, close } = price;
      const dateStr = new Date(param.time * 1000).toLocaleString();
      tooltip.innerHTML = `
        <div><strong>${dateStr}</strong></div>
        <div>O: ${open?.toFixed(5)}</div>
        <div>H: ${high?.toFixed(5)}</div>
        <div>L: ${low?.toFixed(5)}</div>
        <div>C: ${close?.toFixed(5)}</div>
      `;
      const { x, y } = param.point;
      tooltip.style.left = x + 10 + 'px';
      tooltip.style.top = y + 10 + 'px';
    };

    chart.subscribeCrosshairMove(handleCrosshairMove);

    // Resize
    const handleResize = () => {
      if (!chartContainerRef.current) return;
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      if (tooltip) {
        tooltip.remove();
      }
      chart.remove();
    };
  }, [candles]);

  return <div ref={chartContainerRef} style={{ position: 'relative', width: '100%' }} />;
}

/**
 * RsiChart: Zeichnet den RSI als Line-Chart von 0..100
 */
function RsiChart({ rsi }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const tooltipRef = useRef(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 200,
      layout: {
        background: { type: 'Solid', color: '#fff' },
        textColor: '#333'
      },
      crosshair: {
        mode: CrosshairMode.Normal
      },
      leftPriceScale: {
        visible: false
      },
      rightPriceScale: {
        visible: true
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false
      }
    });
    chartRef.current = chart;

    const lineSeries = chart.addLineSeries({
      lineWidth: 2,
      color: 'blue'
    });
    seriesRef.current = lineSeries;

    // Konvertieren
    const rsiData = rsi.map(d => ({
      time: Math.floor(new Date(d.time).getTime() / 1000),
      value: d.value
    }));
    lineSeries.setData(rsiData);

    // Price lines for RSI
    lineSeries.createPriceLine({
      price: 30,
      color: 'red',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: '30'
    });
    lineSeries.createPriceLine({
      price: 70,
      color: 'red',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: '70'
    });

    // Tooltip
    const tooltip = document.createElement('div');
    tooltip.style = `
      position: absolute;
      display: none;
      pointer-events: none;
      background: rgba(0, 0, 0, 0.7);
      color: #fff;
      padding: 5px;
      border-radius: 4px;
      font-size: 12px;
      z-index: 9999;
    `;
    containerRef.current.appendChild(tooltip);
    tooltipRef.current = tooltip;

    const handleCrosshairMove = (param) => {
      if (!param.point || !param.time || !param.seriesPrices.size) {
        tooltip.style.display = 'none';
        return;
      }
      const price = param.seriesPrices.get(lineSeries);
      if (price === undefined) {
        tooltip.style.display = 'none';
        return;
      }
      tooltip.style.display = 'block';
      const dateStr = new Date(param.time * 1000).toLocaleString();
      tooltip.innerHTML = `
        <div><strong>${dateStr}</strong></div>
        <div>RSI: ${price?.toFixed(2)}</div>
      `;
      const { x, y } = param.point;
      tooltip.style.left = x + 10 + 'px';
      tooltip.style.top = y + 10 + 'px';
    };
    chart.subscribeCrosshairMove(handleCrosshairMove);

    const handleResize = () => {
      chart.applyOptions({ width: containerRef.current.clientWidth });
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      if (tooltip) tooltip.remove();
      chart.remove();
    };
  }, [rsi]);

  return <div ref={containerRef} style={{ position: 'relative', width: '100%' }} />;
}

/**
 * Tabelle
 */
function CandleTable({ candles, rsi }) {
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: '1rem' }}>
        <thead>
          <tr style={{ backgroundColor: '#eee' }}>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>Timestamp</th>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>Open</th>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>High</th>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>Low</th>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>Close</th>
            <th style={{ border: '1px solid #ccc', padding: '4px' }}>Volume</th>
            {rsi && rsi.length > 0 && <th style={{ border: '1px solid #ccc', padding: '4px' }}>RSI</th>}
          </tr>
        </thead>
        <tbody>
          {candles.map((candle, idx) => {
            const rsiObj = rsi?.find(r => new Date(r.time).getTime() === new Date(candle.bucket).getTime());
            return (
              <tr key={idx}>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>
                  {new Date(candle.bucket).toLocaleString()}
                </td>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>{candle.open}</td>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>{candle.high}</td>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>{candle.low}</td>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>{candle.close}</td>
                <td style={{ border: '1px solid #ccc', padding: '4px' }}>{candle.volume}</td>
                {rsi && rsi.length > 0 && (
                  <td style={{ border: '1px solid #ccc', padding: '4px' }}>
                    {rsiObj ? rsiObj.value.toFixed(2) : ''}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Hier das Haupt-Component, das du in App.jsx rendern kannst.
 */
export default function DataView() {
  const [asset, setAsset] = useState('EURUSD');
  const [timeframe, setTimeframe] = useState('M10');
  const [start, setStart] = useState(() => {
    // Default Start: vor 30 Tagen
    const now = new Date();
    const past = new Date(now.getTime() - 30 * 24 * 3600 * 1000);
    return toDateTimeLocalString(past);
  });
  const [end, setEnd] = useState(() => {
    // Default End: jetzt
    const now = new Date();
    return toDateTimeLocalString(now);
  });
  const [indicator, setIndicator] = useState('none'); // z.B. 'none' oder 'rsi'
  const [rsiPeriod, setRsiPeriod] = useState(14);

  const [candles, setCandles] = useState([]);
  const [rsiData, setRsiData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Dukascopy-Instrumente
  const DUKASCOPY_ASSETS = [
    'AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD',
    'CADCHF','CADJPY','CHFJPY','EURAUD','EURCAD',
    'EURCHF','EURGBP','EURJPY','EURNZD','EURUSD',
    'GBPAUD','GBPCAD','GBPCHF','GBPJPY','GBPNZD',
    'GBPUSD','NZDCAD','NZDCHF','NZDJPY','NZDUSD',
    'USDCAD','USDCHF','USDJPY'
  ];

  // Erweitertes Timeframe
  const TIMEFRAMES = [
    'M2','M3','M4','M6','M7','M10','M12','M15','M20','M30','H1','H2','H4','D1'
  ];

  const handleFetchData = async (e) => {
    e.preventDefault();
    setError('');
    setCandles([]);
    setRsiData([]);
    setLoading(true);

    try {
      const params = new URLSearchParams({
        asset,
        resolution: timeframe,
        start: new Date(start).toISOString(),
        end: new Date(end).toISOString()
      });
      if (indicator === 'rsi') {
        params.set('indicator', 'rsi');
        params.set('period', String(rsiPeriod));
      }
      const res = await fetch(`/api/candles?${params.toString()}`);
      if (!res.ok) {
        throw new Error(`Fehler vom Server: ${res.statusText}`);
      }
      const json = await res.json();
      // Wir erwarten { candles: [...], rsi: [...] } falls rsi
      setCandles(json.candles || []);
      setRsiData(json.rsi || []);
    } catch (err) {
      setError(err.message);
    }
    setLoading(false);
  };

  return (
    <div style={{ padding: '1rem' }}>
      {/* Top-Bar, einzeilig, 40px hoch */}
      <form
        onSubmit={handleFetchData}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          margin: '0',
          padding: '0',
          height: '40px'
        }}
      >
        <label style={{ margin: 0 }}>Asset</label>
        <select value={asset} onChange={(e) => setAsset(e.target.value)} style={{ margin: 0, height: '24px' }}>
          {DUKASCOPY_ASSETS.map(sym => (
            <option key={sym} value={sym}>{sym}</option>
          ))}
        </select>

        <label style={{ margin: 0 }}>Timeframe</label>
        <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)} style={{ margin: 0, height: '24px' }}>
          {TIMEFRAMES.map(tf => (
            <option key={tf} value={tf}>{tf}</option>
          ))}
        </select>

        <label style={{ margin: 0 }}>Von</label>
        <input
          type="datetime-local"
          value={start}
          onChange={(e) => setStart(e.target.value)}
          style={{ margin: 0, height: '24px' }}
        />

        <label style={{ margin: 0 }}>Bis</label>
        <input
          type="datetime-local"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
          style={{ margin: 0, height: '24px' }}
        />

        <label style={{ margin: 0 }}>Indikator</label>
        <select
          value={indicator}
          onChange={(e) => setIndicator(e.target.value)}
          style={{ margin: 0, height: '24px' }}
        >
          <option value="none">Keiner</option>
          <option value="rsi">RSI</option>
        </select>

        {indicator === 'rsi' && (
          <>
            <label style={{ margin: 0 }}>Periode</label>
            <input
              type="number"
              min="1"
              value={rsiPeriod}
              onChange={(e) => setRsiPeriod(Number(e.target.value))}
              style={{ margin: 0, height: '24px', width: '60px' }}
            />
          </>
        )}

        <button type="submit" style={{ margin: 0, height: '28px', cursor: 'pointer' }}>
          refresh
        </button>
      </form>

      {loading && <p>Lädt...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}

      {/* Collapsible Panels */}
      <CollapsiblePanel title="Chart" defaultOpen={true}>
        <CandleChart candles={candles} />
      </CollapsiblePanel>

      {indicator === 'rsi' && rsiData.length > 0 && (
        <CollapsiblePanel title="RSI Chart" defaultOpen={true}>
          <RsiChart rsi={rsiData} />
        </CollapsiblePanel>
      )}

      <CollapsiblePanel title="Tabelle" defaultOpen={true}>
        <CandleTable candles={candles} rsi={indicator === 'rsi' ? rsiData : []} />
      </CollapsiblePanel>
    </div>
  );
}

/**
 * Kleine Hilfsfunktion: date -> YYYY-MM-DDTHH:mm
 */
function toDateTimeLocalString(date) {
  const pad = (n) => n.toString().padStart(2, '0');
  const YYYY = date.getFullYear();
  const MM = pad(date.getMonth() + 1);
  const DD = pad(date.getDate());
  const hh = pad(date.getHours());
  const mm = pad(date.getMinutes());
  return `${YYYY}-${MM}-${DD}T${hh}:${mm}`;
}
