import React, { useEffect, useRef, useState } from 'react';
import Card from 'react-bootstrap/Card';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { FilterChartIcon, OptionsIcon } from '../../../public/asset/icon';
import { createMultiLineChart, createPercentBarChart, createPercentMultiLineChart } from './helpers';

export enum ChartType {
  percentMultiLine,
  multiLine,
  bar,
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
export function ChartFactory({cardId, title, data, config, type}: props) {
  const refContainer = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  
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
    } else if (type === ChartType.percentMultiLine) {
      createPercentMultiLineChart(cardId, data, dimensions, margins, config.catColors)
    } else if (type === ChartType.bar) {
      createPercentBarChart(cardId, data, dimensions, margins)
    }
  }

  useEffect(() => {
    if (dimensions.width === 0) {
      return;
    }

    createGraph(cardId, data, dimensions);
  }, [cardId, data, dimensions]);

  useEffect(() => {
    if (refContainer.current) {
      setDimensions({
        width: refContainer.current.offsetWidth,
        height: refContainer.current.offsetWidth * 0.6,
      });
    }
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
        <div id={`${cardId}-line`} ref={refContainer} />
      </Card.Body>
    </Card>
  )
}
