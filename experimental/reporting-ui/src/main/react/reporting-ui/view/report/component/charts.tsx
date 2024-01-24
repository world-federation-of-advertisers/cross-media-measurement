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

import React from 'react';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import { TotalReach } from '../../../component/chart/total_reach/total_reach';
import { UniqueReqchByPlat } from '../../../component/chart/unique_reach_by_plat/unique_reach_by_plat';
import { ChartGroup } from '../../../view_model/report/report_view_model';
import { Impressions } from '../../../component/chart/impressions/impressions';
import { Frequencies } from '../../../component/chart/frequencies/frequencies';

type ChartProps = {
  impressions: ChartGroup[],
  cumulativeImpressions: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  totalCumulativeReach: ChartGroup[],
  frequencies: ChartGroup[],
  pubColors: { [Name: string]: string},
}

export function Charts({
  impressions,
  cumulativeImpressions,
  uniqueReach,
  totalReach,
  totalCumulativeReach,
  frequencies,
  pubColors,
}: ChartProps) {
  const smSize = 6;
    return (
      <React.Fragment>
        <Row className="report-charts">
          <Col className="report-chart mb-3" sm={smSize}>
            <Impressions id={'impressions'} impressions={impressions} pubColors={pubColors} />
          </Col>
          <Col className="report-chart mb-3" sm={smSize}>
            <Impressions id={'impressions-cumulative'} title={'Cumulative Impressions'} impressions={cumulativeImpressions} pubColors={pubColors} />
          </Col>
          <Col className="report-chart mb-3" sm={smSize}>
            <TotalReach id={'total-reach'} reach={totalReach} pubColors={pubColors} />
          </Col>
          <Col className="report-chart mb-3" sm={smSize}>
            <TotalReach id={'total-reach-cumulative'} title={'Total Cumulative Reach'} reach={totalCumulativeReach} pubColors={pubColors} />
          </Col>
          <Col className="report-chart mb-3" sm={12}>
            <UniqueReqchByPlat
              id={'uniqueReach'}
              reach={uniqueReach}
              pubColors={pubColors}
            />
          </Col>
          <Col className="report-chart mb-3" sm={12}>
            <Frequencies id={'frequencies'} frequencies={frequencies} pubColors={pubColors} />
          </Col>
        </Row>
      </React.Fragment>
    )
}
