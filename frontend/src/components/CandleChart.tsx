import React from 'react';
import ReactECharts from 'echarts-for-react';

// Define the Candle interface
export interface Candle {
  bucket: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface CandleChartProps {
  candles: Candle[];
}

const CandleChart: React.FC<CandleChartProps> = ({ candles }) => {
  // Prepare the x-axis labels (categories) using the bucket property
  const categoryData = candles.map(c => c.bucket);
  
  // Prepare candlestick values for ECharts
  // ECharts expects each data item as [open, close, low, high]
  const values = candles.map(c => [c.open, c.close, c.low, c.high]);

  const option = {
    title: {
      text: 'Candlestick Chart',
      left: 0
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      }
    },
    legend: {
      data: ['Candlestick']
    },
    toolbox: {
      feature: {
        dataZoom: { yAxisIndex: 'none' },
        restore: {},
        saveAsImage: {}
      }
    },
    grid: {
      left: '10%',
      right: '10%',
      bottom: '15%'
    },
    xAxis: {
      type: 'category',
      data: categoryData,
      boundaryGap: false,
      axisLine: { onZero: false },
      splitLine: { show: false },
      min: 'dataMin',
      max: 'dataMax'
    },
    yAxis: {
      scale: true,
      splitArea: {
        show: true
      }
    },
    dataZoom: [
      {
        type: 'inside',
        start: 50,
        end: 100
      },
      {
        show: true,
        type: 'slider',
        top: '90%',
        start: 50,
        end: 100
      }
    ],
    brush: {
      toolbox: ['rect', 'polygon', 'lineX', 'lineY', 'keep', 'clear']
    },
    series: [
      {
        name: 'Candlestick',
        type: 'candlestick',
        data: values,
        itemStyle: {
          color: '#ec0000',     // up color
          color0: '#00da3c',    // down color
          borderColor: '#8A0000',
          borderColor0: '#008F28'
        },
        markPoint: {
          label: {
            formatter: function (param: any) {
              return param != null ? Math.round(param.value) + '' : '';
            }
          },
          data: [
            {
              name: 'Mark',
              // Using a mid-point as an example coordinate
              coord: [categoryData[Math.floor(categoryData.length / 2)], values[Math.floor(values.length / 2)][0]],
              value: values[Math.floor(values.length / 2)][0],
              itemStyle: {
                color: 'rgb(41,60,85)'
              }
            },
            {
              name: 'Highest',
              type: 'max',
              valueDim: 'highest'
            },
            {
              name: 'Lowest',
              type: 'min',
              valueDim: 'lowest'
            },
            {
              name: 'Avg Close',
              type: 'average',
              valueDim: 'close'
            }
          ],
          tooltip: {
            formatter: function (param: any) {
              return param.name + '<br>' + (param.data.coord || '');
            }
          }
        },
        markLine: {
          symbol: ['none', 'none'],
          data: [
            [
              {
                name: 'From lowest to highest',
                type: 'min',
                valueDim: 'lowest',
                symbol: 'circle',
                symbolSize: 10,
                label: { show: true },
                emphasis: { label: { show: false } }
              },
              {
                type: 'max',
                valueDim: 'highest',
                symbol: 'circle',
                symbolSize: 10,
                label: { show: true },
                emphasis: { label: { show: false } }
              }
            ],
            {
              name: 'Min close line',
              type: 'min',
              valueDim: 'close'
            },
            {
              name: 'Max close line',
              type: 'max',
              valueDim: 'close'
            }
          ]
        }
      }
    ]
  };

  return <ReactECharts option={option} style={{ height: '400px', width: '100%' }} />;
};

export default CandleChart;
