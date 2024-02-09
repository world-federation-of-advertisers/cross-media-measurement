// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React, { useEffect, useRef, useState } from 'react';
import Card from 'react-bootstrap/Card';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { FilterChartIcon, OptionsIcon } from '../../public/asset/icon';
import {
  createLegend,
  createMultiLineChart,
  createBarChart,
  createPercentBarChart,
  createPercentMultiLineChart,
  removeGraph
} from './d3_wrapper';

export enum ChartType {
  percentMultiLine,
  multiLine,
  bar,
  barPercent,
}

type props = {
  cardId: string,
  title: string,
  data: any,
  config: any,
  type: ChartType,
}

const componentStyle = {
  borderRadius: '12px',
  boxShadow: '0px 2px 6px 2px rgba(0, 0, 0, 0.15), 0px 1px 2px 0px rgba(0, 0, 0, 0.30)',
}

// TODO: Add Legend
export function Chart({cardId, title, data, config, type}: props) {
  const chartRefContainer = useRef<HTMLDivElement>(null);
  const legendRefContainer = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [legendDimensions, setLegendDimensions] = useState({ width: 0, height: 0 });

  const createGraph = (cardId: string, data: any, dimensions: {width: number, height: number}) => {
    // Specify the chart’s dimensions.
    const margins ={
      top: 20,
      right: 20,
      bottom: 30,
      left: 30,
    }

    if (type === ChartType.multiLine) {
      createMultiLineChart(cardId, data, dimensions, margins, config.pubColors)
      createLegend(cardId, legendDimensions, config.pubColors);
    } else if (type === ChartType.percentMultiLine) {
      createPercentMultiLineChart(cardId, data, dimensions, margins, config.catColors)
    } else if (type === ChartType.barPercent) {
      createPercentBarChart(cardId, data, dimensions, margins, config.pubColors)
    } else if (type === ChartType.bar) {
      createBarChart(cardId, data, dimensions, margins, config.pubColors)
      createLegend(cardId, legendDimensions, config.pubColors);
    }
  }

  useEffect(() => {
    if (dimensions.width === 0) {
      return;
    }

    createGraph(cardId, data, dimensions);
  }, [cardId, data, dimensions]);

  const resize = (container: React.RefObject<HTMLDivElement>, setDimensions: Function) => {
    if (container.current) {
      setDimensions({
        width: container.current.offsetWidth,
        height: Math.min(container.current.offsetWidth * 0.6, 300),
      });
    }
  }

  const resizeLegend = (container: React.RefObject<HTMLDivElement>, setDimensions: Function) => {
    if (container.current) {
      setDimensions({
        width: 100,
        height: 100,
      });
    }
  }

  useEffect(() => {
    resize(chartRefContainer, setDimensions);
    resizeLegend(legendRefContainer, setLegendDimensions);

    function handleResize() {
      // Delete and re-create the whole charts.
      // Even when using 'responsive svg', the fonts don't change.
      removeGraph(cardId);
      resize(chartRefContainer, setDimensions);
      resizeLegend(legendRefContainer, setLegendDimensions);
    }

    // Attach the event listener to the window object
    window.addEventListener('resize', handleResize);

    // Remove the event listener when the component unmounts
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, []);

  return (
    <Card id={cardId} style={componentStyle}>
      <Card.Body>
        <div style={{borderBottom: '1px solid #E1E3E1'}}>
          <Row>
            <Col className="my-auto">
              {title}
            </Col>
            <Col md="auto" className="my-auto">
              <FilterChartIcon />
            </Col>
            <Col md="auto" className="my-auto">
              <OptionsIcon />
            </Col>
          </Row>
        </div>
        <div id={`${cardId}-chart`} className="chart-card" ref={chartRefContainer} />
        <div id={`${cardId}-legend`} className="legend" ref={legendRefContainer} />
      </Card.Body>
    </Card>
  )
}
