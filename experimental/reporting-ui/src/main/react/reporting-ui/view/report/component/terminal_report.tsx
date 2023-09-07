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

const COLORS = Object.freeze([
  '#FFA300',
  '#EA5F94',
  '#9D02D7',
  '#5E5E5E',
]);

export const TerminalReport = ({name, overview, summaries}) => {
  // Assign a color to a publisher
  const pub_ids = summaries.map(x => x.id);
  const pub_colors = {};
  COLORS.forEach((x, i) => pub_colors[pub_ids[i]] = x);

  return (
    <React.Fragment>
      <Header reportName={name} />
      <ReportOverviewStats reportOverview={overview} />
      <SummaryTable reportSummaries={summaries} publisherColors={pub_colors} />
      {/* TODO: Add the charts */}
    </React.Fragment>
  )
};
