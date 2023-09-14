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
import {
  DedupedReachByDemo,
  OnTargetReach,
  OnTargetUniqueReachByPlat,
  OnTargetXmediaReachByFreq,
  TotalReach,
  UniqueReqchByPlat,
  XmediaReachByFreq,
} from '../../../component/chart/chart_type';
import { Demographic, Reach, TargetReach, UniqueReach } from '../../../model/reporting';

type ChartProps = {
  targetReach: Reach[],
  totalReach: Reach[],
  xmediaReach: TargetReach[],
  onTargetReach: TargetReach[],
  demo: Demographic[],
  onTargetUniqueReach: UniqueReach[],
  uniqueReachByPlat: UniqueReach[],
  pubColors: { [Name: string]: string},
}

export function Charts({
  targetReach,
  totalReach,
  xmediaReach,
  onTargetReach,
  demo,
  onTargetUniqueReach,
  uniqueReachByPlat,
  pubColors,
}: ChartProps) {
    return (
      <React.Fragment>
        <Row className="report-charts">
          <Col className="report-chart">
            <OnTargetReach id={'target-reach'} reach={targetReach} pubColors={pubColors} />
          </Col>
          <Col className="report-chart">
            <TotalReach id={'total-reach'} reach={totalReach} pubColors={pubColors} />
          </Col>
        </Row>
        <Row className="report-charts">
          <Col className="report-chart">
            <OnTargetXmediaReachByFreq
              id={'target-xmedia-reach'}
              reach={onTargetReach}
            />
          </Col>
          <Col className="report-chart">
            <XmediaReachByFreq
              id={'xmedia-reach'}
              reach={xmediaReach}
            />
          </Col>
        </Row>
        <Row className="report-charts">
          <Col className="report-chart">
            <DedupedReachByDemo
              id={'deduped-reach'}
              demo={demo}
              pubColors={pubColors}
            />
          </Col>
        </Row>
        <Row className="report-charts">
          <Col className="report-chart">
            <OnTargetUniqueReachByPlat
              id={'target-unique-reach'}
              reach={onTargetUniqueReach}
              pubColors={pubColors}
            />
          </Col>
          <Col className="report-chart">
            <UniqueReqchByPlat
              id={'id7'}
              reach={uniqueReachByPlat}
              pubColors={pubColors}
            />
          </Col>
        </Row>
      </React.Fragment>
    )
}
