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
import { Header } from './header';
import { ReportOverviewStats } from './overview';
import { SummaryTable } from './summary_table';
import { Demographic, Overview, Reach, SummaryPublisherData, TargetReach, UniqueReach } from '../../../model/reporting';
import { Charts } from './charts';

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
  targetReach: Reach[],
  totalReach: Reach[],
  xmediaReach: TargetReach[],
  onTargetReach: TargetReach[],
  onTargetUniqueReach: UniqueReach[],
  uniqueReachByPlat: UniqueReach[],
  demo: Demographic[],
}

export const TerminalReport = ({
  name,
  overview,
  summaries,
  targetReach,
  totalReach,
  xmediaReach,
  onTargetReach,
  onTargetUniqueReach,
  uniqueReachByPlat,
  demo,
}: TerminalReportProps) => {
  // Assign a color to each publisher
  const pubIds = summaries.map(x => x.id);
  const pubColors = {};
  pubIds.forEach((pub_id, index) => pubColors[pub_id] = COLORS[index]);

  return (
    <React.Fragment>
      <Header reportName={name} />
      <ReportOverviewStats reportOverview={overview} />
      <SummaryTable reportSummaries={summaries} publisherColors={pubColors} />
      <Charts
        targetReach={targetReach}
        totalReach={totalReach}
        xmediaReach={xmediaReach}
        onTargetReach={onTargetReach}
        demo={demo}
        onTargetUniqueReach={onTargetUniqueReach}
        uniqueReachByPlat={uniqueReachByPlat}
        pubColors={pubColors}
      />
    </React.Fragment>
  )
};
