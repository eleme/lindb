/*
Licensed to LinDB under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. LinDB licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
import ChartLegend from "@src/components/chart/ChartLegend";
import { DefaultChartConfig } from "@src/configs";
import { ChartStatus, ChartTypeEnum, UnitEnum } from "@src/models";
import { ChartEventStore, ChartStore } from "@src/stores";
import urlStore from "@src/stores/url.store";
import { setStyle } from "@src/utils";
import Chart from "chart.js/auto";
import * as _ from "lodash-es";
import { reaction } from "mobx";
import moment from "moment";
import React, { MutableRefObject, useEffect, useRef } from "react";
import { DateTimeFormat } from "@src/constants";

interface CanvasChartProps {
  chartId: string;
  height?: number;
}
const Zoom = {
  drag: false,
  isMouseDown: false,
  selectedStart: 0,
  selectedEnd: 0,
};

export default function CanvasChart(props: CanvasChartProps) {
  const { chartId, height } = props;
  const eventCallbacks: Map<string, any> = new Map();
  const chartRef = useRef() as MutableRefObject<HTMLCanvasElement | null>;
  const crosshairRef = useRef() as MutableRefObject<HTMLDivElement>;
  const chartObjRef = useRef() as MutableRefObject<Chart | null>;
  const zoomRef = useRef(_.cloneDeep(Zoom));
  const zoomDivRef = useRef() as MutableRefObject<HTMLDivElement>;
  const seriesRef = useRef() as MutableRefObject<any>;
  const chartStatusRef = useRef() as MutableRefObject<ChartStatus | undefined>;

  const createChart = () => {
    // console.log("config", { type: "line", data: series }, config);
    const canvas = chartRef.current;
    if (!canvas) {
      return;
    }
    const chartCfg = ChartStore.charts.get(chartId);
    const config: any = _.merge(
      {
        type: _.get(chartCfg, "type", ChartTypeEnum.Line),
        unit: _.get(chartCfg, "unit", UnitEnum.None),
      },
      DefaultChartConfig
    );

    const chartInstance = new Chart(canvas, config);
    chartObjRef.current = chartInstance;
    let start = 0;

    eventCallbacks.set("mousemove", function (e: MouseEvent) {
      if (chartStatusRef.current != ChartStatus.OK) {
        return;
      }
      const points: any = chartInstance.getElementsAtEventForMode(
        e,
        "index",
        { intersect: false },
        false
      );
      if (!points || points.length <= 0) {
        return;
      }
      const chartArea = chartInstance.chartArea;
      const currIdx = points[0].index;
      const height = chartArea.height;
      const top = chartArea.top;
      const x = e.offsetX;
      if (zoomRef.current.isMouseDown) {
        zoomRef.current.selectedEnd = seriesRef.current.times[points[0].index];
        zoomRef.current.drag = true;
        const width = e.offsetX - start;
        if (width >= 0) {
          setStyle(zoomDivRef.current, {
            width: `${width}px`,
          });
        } else {
          setStyle(zoomDivRef.current, {
            width: `${-width}px`,
            transform: `translate(${e.offsetX}px, ${chartArea.top}px)`,
          });
        }
      }

      const canvaxRect = canvas.getBoundingClientRect();

      // console.log("heeee", e);
      setStyle(crosshairRef.current, {
        display: "block",
        height: `${height}px`,
        transform: `translate(${x}px, ${top}px)`,
      });
      ChartEventStore.setShowTooltip(true);
      ChartEventStore.mouseMove({
        index: currIdx,
        mouseX: x,
        chart: chartInstance,
        chartArea: chartArea,
        chartCanvas: canvas,
        chartCanvasRect: canvaxRect,
        nativeEvent: e,
      });
    });
    eventCallbacks.set("mouseleave", function (e: any) {
      if (chartStatusRef.current != ChartStatus.OK) {
        return;
      }
      setStyle(crosshairRef.current, {
        display: "none",
      });
      ChartEventStore.mouseLeave(e);
      ChartEventStore.setShowTooltip(false);
    });
    eventCallbacks.set("mousedown", function (e: any) {
      if (chartStatusRef.current != ChartStatus.OK) {
        return;
      }
      zoomRef.current.isMouseDown = true;
      const points: any = chartInstance.getElementsAtEventForMode(
        e,
        "index",
        { intersect: false },
        false
      );
      if (points && points.length > 0) {
        zoomRef.current.selectedStart =
          seriesRef.current.times[points[0].index];
      }
      start = e.offsetX;
      const chartArea = chartInstance.chartArea;
      const height = chartArea.height;
      setStyle(zoomDivRef.current, {
        display: "block",
        height: `${height}px`,
        // left: start,
        transform: `translate(${start}px, ${chartArea.top}px)`,
      });
    });
    eventCallbacks.set("mouseup", function (_e: any) {
      console.log("uppppppppppppp");
      if (chartStatusRef.current != ChartStatus.OK) {
        return;
      }
      setStyle(zoomDivRef.current, {
        display: "none",
        width: "0px",
      });
      zoomRef.current.isMouseDown = false;
      if (zoomRef.current.drag) {
        const start = Math.min(
          zoomRef.current.selectedStart,
          zoomRef.current.selectedEnd
        );
        const end = Math.max(
          zoomRef.current.selectedStart,
          zoomRef.current.selectedEnd
        );
        const from = moment(start).format(DateTimeFormat);
        const to = moment(end).format(DateTimeFormat);
        urlStore.changeURLParams({ params: { from: from, to: to } });
      }

      zoomRef.current.drag = false;
      zoomRef.current.selectedStart = 0;
      zoomRef.current.selectedEnd = 0;
    });

    eventCallbacks.forEach((v, k) => {
      console.log("canvas......", canvas);
      canvas.addEventListener(k, v);
    });
  };

  /**
   * set chart display data
   * @param series data which display in chart
   */
  const setChartData = (series: any) => {
    const chartInstance = chartObjRef.current;
    if (chartInstance) {
      chartInstance.data = series;
      // _.set(
      //   config,
      //   "options.scales.y.ticks.suggestedMax",
      //   _.get(series, "leftMax", 0) * 1.5
      // );
      console.log("set chart data", series, _.get(series, "leftMax", 0));
    }
  };

  /**
   * Init canvas chart component.
   * 1. watch chart status from ChartStore.
   */
  useEffect(() => {
    const disposer = [
      reaction(
        () => ChartStore.chartStatusMap.get(chartId),
        (s: ChartStatus | undefined) => {
          chartStatusRef.current = s;
          if (!s || s == ChartStatus.Loading) {
            return;
          }
          const series = ChartStore.seriesCache.get(chartId);
          seriesRef.current = series;
          const chartInstance = chartObjRef.current;

          if (chartInstance) {
            setChartData(series);
            chartInstance.update();
          } else {
            createChart();
            setChartData(series);
          }
          console.log("status", s);
        }
      ),
    ];
    const canvas = chartRef.current;

    return () => {
      if (canvas) {
        eventCallbacks.forEach((v, k) => {
          canvas.removeEventListener(k, v);
        });
      }
      disposer.forEach((d) => d());
      if (chartObjRef.current) {
        chartObjRef.current.destroy();
        // reset chart obj as null, maybe after hot load(develop) canvas element not ready.
        // invoke chart update will fail.
        chartObjRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div>
      <div className="lin-chart" style={{ height: height || 200 }}>
        <canvas className="chart" ref={chartRef} height={height || 200} />
        <div ref={crosshairRef} className="crosshair" />
        <div ref={zoomDivRef} className="zoom" />
      </div>
      <ChartLegend />
    </div>
  );
}
