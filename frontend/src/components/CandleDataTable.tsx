import React from 'react';
import { Candle } from './CandleChart'; // Wiederverwendung des Candle-Interfaces

interface CandleDataTableProps {
  candles: Candle[];
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  marginTop: '1rem'
};

const thStyle: React.CSSProperties = {
  borderBottom: '2px solid #ddd',
  padding: '8px',
  textAlign: 'left',
  backgroundColor: '#f2f2f2'
};

const tdStyle: React.CSSProperties = {
  borderBottom: '1px solid #ddd',
  padding: '8px'
};

const CandleDataTable: React.FC<CandleDataTableProps> = ({ candles }) => {
  return (
    <table style={tableStyle}>
      <thead>
        <tr>
          <th style={thStyle}>Bucket</th>
          <th style={thStyle}>Open</th>
          <th style={thStyle}>High</th>
          <th style={thStyle}>Low</th>
          <th style={thStyle}>Close</th>
          <th style={thStyle}>Volume</th>
        </tr>
      </thead>
      <tbody>
        {candles.map((candle, index) => (
          <tr key={index}>
            <td style={tdStyle}>{candle.bucket}</td>
            <td style={tdStyle}>{candle.open}</td>
            <td style={tdStyle}>{candle.high}</td>
            <td style={tdStyle}>{candle.low}</td>
            <td style={tdStyle}>{candle.close}</td>
            <td style={tdStyle}>{candle.volume}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
};

export default CandleDataTable;
