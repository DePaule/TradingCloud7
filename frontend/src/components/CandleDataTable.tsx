import React, { useState, useMemo } from 'react';
import { Candle } from './CandleChart';

interface CandleDataTableProps {
  candles: Candle[];
  onSignalChange: (bucket: string, newSignal: number | null) => void;
}

type SortColumn = keyof Omit<Candle, 'signal'> | 'signal';
type SortDirection = 'asc' | 'desc';

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  marginTop: '1rem'
};

const thStyle: React.CSSProperties = {
  borderBottom: '2px solid #ddd',
  padding: '8px',
  textAlign: 'left',
  backgroundColor: '#f2f2f2',
  cursor: 'pointer'
};

const tdStyle: React.CSSProperties = {
  borderBottom: '1px solid #ddd',
  padding: '8px'
};

const CandleDataTable: React.FC<CandleDataTableProps> = ({ candles, onSignalChange }) => {
  const [sortColumn, setSortColumn] = useState<SortColumn>('bucket');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');

  const handleSort = (column: SortColumn) => {
    if (column === sortColumn) {
      // Toggle sort direction if the same column is clicked
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const sortedCandles = useMemo(() => {
    return [...candles].sort((a, b) => {
      let aValue = a[sortColumn] ?? 0;
      let bValue = b[sortColumn] ?? 0;
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortDirection === 'asc'
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      } else if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
      }
      return 0;
    });
  }, [candles, sortColumn, sortDirection]);

  // Helper function to display sort indicator
  const renderSortIndicator = (column: SortColumn) => {
    if (column !== sortColumn) return null;
    return sortDirection === 'asc' ? ' ▲' : ' ▼';
  };

  return (
    <table style={tableStyle}>
      <thead>
        <tr>
          <th style={thStyle} onClick={() => handleSort('bucket')}>
            Date & Time{renderSortIndicator('bucket')}
          </th>
          <th style={thStyle} onClick={() => handleSort('open')}>
            Open{renderSortIndicator('open')}
          </th>
          <th style={thStyle} onClick={() => handleSort('high')}>
            High{renderSortIndicator('high')}
          </th>
          <th style={thStyle} onClick={() => handleSort('low')}>
            Low{renderSortIndicator('low')}
          </th>
          <th style={thStyle} onClick={() => handleSort('close')}>
            Close{renderSortIndicator('close')}
          </th>
          <th style={thStyle} onClick={() => handleSort('volume')}>
            Volume{renderSortIndicator('volume')}
          </th>
          <th style={thStyle} onClick={() => handleSort('signal')}>
            Signal{renderSortIndicator('signal')}
          </th>
        </tr>
      </thead>
      <tbody>
        {sortedCandles.map((candle) => (
          <tr key={candle.bucket}>
            {/* Show full date and time */}
            <td style={tdStyle}>{new Date(candle.bucket).toLocaleString()}</td>
            <td style={tdStyle}>{candle.open}</td>
            <td style={tdStyle}>{candle.high}</td>
            <td style={tdStyle}>{candle.low}</td>
            <td style={tdStyle}>{candle.close}</td>
            <td style={tdStyle}>{candle.volume}</td>
            <td style={tdStyle}>
              <input
                type="number"
                value={candle.signal !== undefined ? candle.signal : ''}
                onChange={(e) => {
                  const val = e.target.value;
                  const newSignal = val === '' ? null : Number(val);
                  onSignalChange(candle.bucket, newSignal);
                }}
                style={{ width: '60px' }}
              />
            </td>
          </tr>
        ))}
      </tbody>
    </table>    
  );
};

export default CandleDataTable;
