import React from 'react';

interface Candle {
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface TableProps {
  data: Candle[];
}

const TableComponent: React.FC<TableProps> = ({ data }) => {
  return (
    <div style={{ overflowX: 'auto' }}>
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Open</th>
            <th>High</th>
            <th>Low</th>
            <th>Close</th>
            <th>Volume</th>
          </tr>
        </thead>
        <tbody>
          {data.map((candle, index) => (
            <tr key={index}>
              <td>{new Date(candle.bucket).toLocaleString()}</td>
              <td>{candle.open}</td>
              <td>{candle.high}</td>
              <td>{candle.low}</td>
              <td>{candle.close}</td>
              <td>{candle.volume}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default TableComponent;
