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
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import './overview.css';
import { ReportOverviewCard } from './overview_card';
import { Overview } from '../../../model/reporting';

type ReportTotalsProps = {
  reportOverview: Overview;
};

export function ReportOverviewStats({reportOverview}: ReportTotalsProps) {
  return (
    <Row id="report-overview">
      <Col className='mb-3' sm={4}>
        <ReportOverviewCard id='report-overview-impressions-card' title='Cross-Media Impressions' value={reportOverview.totalImpressions}/>
      </Col>
      <Col className='mb-3' sm={4}>
        <ReportOverviewCard id='report-overview-total-reach-card' title='Cross-Media Reach' value={reportOverview.totalReach}/>
      </Col>
      <Col className='mb-3' sm={4}>
        <ReportOverviewCard id='report-overview-frequency-card' title='Average Frequency' value={reportOverview.totalAverageFrequency}/>
      </Col>
      <Col className='mb-3' sm={6}>
        <ReportOverviewCard id='report-overview-frequency-card' title='Date Range' value={`${reportOverview.startDate} - ${reportOverview.endDate}`}/>
      </Col>
    </Row>
  );
}
