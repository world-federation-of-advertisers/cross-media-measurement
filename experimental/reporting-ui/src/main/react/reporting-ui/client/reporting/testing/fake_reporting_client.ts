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

import {
  GetReportRequest,
  GetReportResponse,
  ListReportsResponse,
  Report,
} from '../../../model/reporting';
import { ChartGroup } from '../../../model/chart_group';
import {ReportingClient} from '../client';


function regenerateTimeData() {
  const chartData: ChartGroup[] = [];
  for (let i = 0; i < 7; i++) {
    const date = new Date(Date.UTC(2020, 0, i + 1));
    let rolling = 0;
    for (let pub = 1; pub < 4; pub++) {
      const value = Math.floor(Math.random() * i + 3);
      rolling += value;
      const temp = {
        group: pub.toString(),
        value,
        date,
      };
      chartData.push(temp);
    }
    chartData.push({group: '0', value: rolling, date});
  }

  return chartData;
}

export class FakeReportingClient implements ReportingClient {
  reports: Report[];

  constructor() {
    this.reports = [
      {
        reportId: '1',
        name: 'Winter campaigns report',
        state: 'SUCCEEDED',
        timeInterval: [],
      },
    ];
  }

  listReports(): Promise<ListReportsResponse> {
    const response = Object.freeze({reports: this.reports});
    return Promise.resolve(response);
  }

  createReport(): Promise<void> {
    return Promise.resolve();
  }

  getReport(req: GetReportRequest): Promise<GetReportResponse> {
    const rep = this.reports.find(x => x.reportId === req.id);
    const response = Object.freeze({
      report: rep,
    });
    return Promise.resolve(response);
  }
}
