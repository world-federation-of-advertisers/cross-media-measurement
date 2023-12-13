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
import Row from 'react-bootstrap/Row';
import { TotalReach } from '../../../component/chart/total_reach/total_reach';
import { UniqueReqchByPlat } from '../../../component/chart/unique_reach_by_plat/unique_reach_by_plat';
import { ChartGroup } from '../../../view_model/report/report_view_model';
import { Impressions } from '../../../component/chart/impressions/impressions';
import { Frequencies } from '../../../component/chart/frequencies/frequencies';

type ChartProps = {
  impressions: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  frequencies: ChartGroup[],
  pubColors: { [Name: string]: string},
}

export function Charts({
  impressions,
  uniqueReach,
  totalReach,
  frequencies,
  pubColors,
}: ChartProps) {
    return (
      <React.Fragment>
        <Row className="report-charts">
          <Col className="report-chart">
            <Impressions id={'impressions'} impressions={impressions} pubColors={pubColors} />
          </Col>
          <Col className="report-chart">
            <UniqueReqchByPlat
              id={'uniqueReach'}
              reach={uniqueReach}
              pubColors={pubColors}
            />
          </Col>
        </Row>
        <Row className="report-charts">
          <Col className="report-chart">
            <TotalReach id={'total-reach'} reach={totalReach} pubColors={pubColors} />
          </Col>
          <Col className="report-chart">
            <Frequencies id={'frequencies'} frequencies={frequencies} pubColors={pubColors} />
          </Col>
        </Row>
      </React.Fragment>
    )
}
