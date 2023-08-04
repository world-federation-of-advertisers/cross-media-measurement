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
  InitApiProps,
  ListReportsResponse,
  Report,
  UniqueReach,
} from '../../../../../main/react/reporting-ui/library/reporting_client/models';
import { ReportingClient } from '../../../../../main/react/reporting-ui/library/reporting_client/reporting_client';

function regenerateTimeData() {
  const chartData: UniqueReach[] = [];
  for (let i = 0; i < 7; i++) {
    const date = new Date(Date.UTC(2020, 0, i + 1));
    let rolling = 0;
    for (let pub = 1; pub < 4; pub++) {
      const value = Math.floor(Math.random() * i + 3);
      rolling += value;
      const temp = {
        pub,
        value,
        date,
      };
      chartData.push(temp);
    }
    chartData.push({pub: 0, value: rolling, date});
  }

  return chartData;
}

export class FakeApi implements ReportingClient {
  reports: Report[];

  constructor() {
    this.reports = [
      {
        id: '1',
        name: 'Winter campaigns report',
        overview: {
          totalImpressions: 23900000,
          totalReach: 6800000,
          totalOnTargetReach: 3300000,
          totalUniqueReach: 0,
          totalAverageFrequency: 8,
        },
        summary: [
          {
            id: 1,
            publisher: 'Pied Piper',
            impressions: 9300000,
            reach: 2200000,
            onTargetReach: 1300000,
            uniqueReach: 589000,
            averageFrequency: 4,
          },
          {
            id: 2,
            publisher: 'Digital Sense',
            impressions: 7800000,
            reach: 1300000,
            onTargetReach: 800000,
            uniqueReach: 233000,
            averageFrequency: 5,
          },
          {
            id: 3,
            publisher: 'Ad Technologies',
            impressions: 6800000,
            reach: 3300000,
            onTargetReach: 1200000,
            uniqueReach: 643000,
            averageFrequency: 16,
          },
        ],
        targetUniqueReachByPlatform: regenerateTimeData(),
        targetXmediaReachByFreq: [
          {
            cat: '1+',
            value: 0,
            x: 0,
          },
          {
            cat: '2+',
            value: 0,
            x: 0,
          },
          {
            cat: '3+',
            value: 0,
            x: 0,
          },
          {
            cat: '1+',
            value: 15,
            x: 8,
          },
          {
            cat: '2+',
            value: 20,
            x: 8,
          },
          {
            cat: '3+',
            value: 25,
            x: 8,
          },
          {
            cat: '1+',
            value: 25,
            x: 12,
          },
          {
            cat: '2+',
            value: 32,
            x: 12,
          },
          {
            cat: '3+',
            value: 40,
            x: 12,
          },
          {
            cat: '1+',
            value: 35,
            x: 16,
          },
          {
            cat: '2+',
            value: 45,
            x: 16,
          },
          {
            cat: '3+',
            value: 55,
            x: 16,
          },
          {
            cat: '1+',
            value: 42,
            x: 20,
          },
          {
            cat: '2+',
            value: 50,
            x: 20,
          },
          {
            cat: '3+',
            value: 57,
            x: 20,
          },
          {
            cat: '1+',
            value: 47,
            x: 24,
          },
          {
            cat: '2+',
            value: 55,
            x: 24,
          },
          {
            cat: '3+',
            value: 62,
            x: 24,
          },
          {
            cat: '1+',
            value: 49,
            x: 28,
          },
          {
            cat: '2+',
            value: 57,
            x: 28,
          },
          {
            cat: '3+',
            value: 64,
            x: 28,
          },
          {
            cat: '1+',
            value: 51,
            x: 32,
          },
          {
            cat: '2+',
            value: 59,
            x: 32,
          },
          {
            cat: '3+',
            value: 66,
            x: 32,
          },
        ],
        totalReach: [
          {
            pub: 0,
            value: 0,
            date: new Date(Date.UTC(2020, 0, 3)),
          },
          {
            pub: 0,
            value: 500000,
            date: new Date(Date.UTC(2020, 0, 4)),
          },
          {
            pub: 0,
            value: 1000000,
            date: new Date(Date.UTC(2020, 0, 5)),
          },
          {
            pub: 0,
            value: 1300000,
            date: new Date(Date.UTC(2020, 0, 6)),
          },
          {
            pub: 0,
            value: 1600000,
            date: new Date(Date.UTC(2020, 0, 7)),
          },
          {
            pub: 0,
            value: 1700000,
            date: new Date(Date.UTC(2020, 0, 8)),
          },
          {
            pub: 0,
            value: 1750000,
            date: new Date(Date.UTC(2020, 0, 9)),
          },
        ],
        targetReach: regenerateTimeData(),
        demo: [
          {
            cat: 'F 18-24',
            val: 40,
          },
          {
            cat: 'F 25-34',
            val: 45,
          },
          {
            cat: 'F 35-44',
            val: 57,
          },
          {
            cat: 'F 45-54',
            val: 75,
          },
          {
            cat: 'F 55-65',
            val: 88,
          },
          {
            cat: 'M 18-24',
            val: 40,
          },
          {
            cat: 'M 25-34',
            val: 45,
          },
          {
            cat: 'M 35-44',
            val: 57,
          },
          {
            cat: 'M 45-54',
            val: 75,
          },
          {
            cat: 'M 55-65',
            val: 88,
          },
        ],
        uniqueReachByPlatform: regenerateTimeData(),
        status: 'status',
      },
    ];
  }

  listReports(): Promise<ListReportsResponse> {
    const response = Object.freeze({reports: this.reports})
    return Promise.resolve(response);
  }

  createReport(): Promise<void> {
    return Promise.resolve();
  }

  getReport(req: GetReportRequest): Promise<GetReportResponse> {
    const rep = this.reports.find(x => x.id === req.id);
    const response = Object.freeze({
      report: rep,
    });
    return Promise.resolve(response);
  }
}
