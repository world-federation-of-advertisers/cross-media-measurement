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
import Alert from 'react-bootstrap/Alert';
import { Header } from './header';
import { ReportOverviewStats } from './overview';
import { SummaryTable } from './summary_table';
import { Overview, SummaryPublisherData } from '../../../model/reporting';
import { Charts } from './charts';
import { ChartGroup } from '../../../model/chart_group';

const COLORS = Object.freeze([
  '#FFA300',
  '#EA5F94',
  '#9D02D7',
  '#5E5E5E',
]);

type TerminalReportProps = {
  name: string;
  overview: Overview,
  summaries: SummaryPublisherData[],
  impressions: ChartGroup[],
  cumulativeImpressions: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  totalCumulativeReach: ChartGroup[],
  frequencies: ChartGroup[],
}

export const TerminalReport = ({
  name,
  overview,
  summaries,
  impressions,
  cumulativeImpressions,
  uniqueReach,
  totalReach,
  totalCumulativeReach,
  frequencies,
}: TerminalReportProps) => {
  // Assign a color to each publisher
  const pubIds = summaries.map(x => x.id);
  const pubColors = {'All EDPs': '#363636'};
  pubIds.forEach((pub_id, index) => pubColors[pub_id] = COLORS[index]);

  return (
    <React.Fragment>
      <div className='sticky'>
        <Header reportName={name} />
        <Alert variant='warning' dismissible>
          This prototype illustrates basic outputs available by using the Halo framework. Local market level outputs and interfaces will vary.
        </Alert>
      </div>
      <ReportOverviewStats reportOverview={overview} />
      <SummaryTable reportSummaries={summaries} publisherColors={pubColors} />
      <Charts
        impressions={impressions}
        cumulativeImpressions={cumulativeImpressions}
        uniqueReach={uniqueReach}
        totalReach={totalReach}
        totalCumulativeReach={totalCumulativeReach}
        frequencies={frequencies}
        pubColors={pubColors}
      />
    </React.Fragment>
  )
};
