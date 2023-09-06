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

const PUB_COLORS = {
  '0': '#5E5E5E',
  '1': '#FFA300',
  '2': '#EA5F94',
  '3': '#9D02D7',
}

export const TerminalReport = ({name, overview, summaries}) => {
  return (
    <React.Fragment>
      <Header reportName={name} />
      <ReportOverviewStats reportOverview={overview} />
      <SummaryTable reportSummaries={summaries} publisherColors={PUB_COLORS} />
      {/* TODO: Add the charts */}
    </React.Fragment>
  )
};
