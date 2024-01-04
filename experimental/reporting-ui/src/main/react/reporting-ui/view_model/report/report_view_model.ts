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

import {useState} from 'react';
import {
  Overview,
  Report,
  SummaryPublisherData,
} from '../../model/reporting';
import { ReportRepository } from '../../model/report/report_repository';

type UiReport = {
  id: string,
  name: string,
  status: string,
  overview: Overview,
  summary: SummaryPublisherData[],
  impressions: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  averageFrequency: ChartGroup[],
}

export type ChartGroup = {
  date: Date|string;
  value: number;
  group: string;
}

type iAndF = {
  impressions: ChartGroup[],
  frequencies: ChartGroup[],
  summary: SummaryPublisherData[],
  overview: any,
}

type Reaches = {
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
}

const getReaches = (report: Report): Reaches => {
  const data: ChartGroup[] = [];
  const totalReach: ChartGroup[] = [];

  report.timeInterval.forEach(ti => {
    ti.demoBucket.forEach(db => {
      const fullUnion = db.unionSource;
      db.perPublisherSource.forEach(pub => {
        totalReach.push({
          value: fixNumber(pub.reach),
          group: `${pub.sourceName}|${db.demoCategoryName}`,
          date: new Date(ti.timeInterval.startTime),
        })
        // TODO: Get complements and calculate: unique reach = union reach - complement reach
      })
      // Object.keys(complements).forEach(name => {
      //   var complement = db.perPublisherSource.find(x => x.sourceName === complements[name]);

      //   data.push({
      //     value: fullUnion.reach - complement.reach,
      //     group: `${name}-${db.demoCategoryName}`,
      //     date: new Date(ti.timeInterval.startTime),
      //   })
      // })
    });
  });

  return {
    uniqueReach: data,
    totalReach,
  };
}

const fixNumber = (num: number): number => {
  const newNum = Number(num);
  return Number.isNaN(newNum) || newNum < 0 ? 0 : newNum;
}

const getImpressionsAndFrequencies = (report: Report): iAndF => {
  const impressions: ChartGroup[] = [];
  const frequencies: ChartGroup[] = [];
  const overview: Overview = {
    totalImpressions: 0,
    totalReach: 0,
    totalAverageFrequency: 0,
    totalOnTargetReach: 0,
    totalUniqueReach: 0,
  }

  // Don't add the complements or union
  const dict: { [id: string] : SummaryPublisherData; } = {};

  report.timeInterval.forEach(ti => {
    ti.demoBucket.forEach(db => {
      db.perPublisherSource.forEach(pps => {
        if (!Object.keys(dict).includes(pps.sourceName)) {
          dict[pps.sourceName] = {
            id: pps.sourceName,
            publisher: pps.sourceName,
            impressions: 0,
            reach: 0,
            onTargetReach: 0,
            uniqueReach: 0,
            averageFrequency: 0,
          }
        }
        dict[pps.sourceName].impressions += fixNumber(pps.impressionCount.count);
        dict[pps.sourceName].averageFrequency += pps.frequencyHistogram[1];
        dict[pps.sourceName].reach += fixNumber(pps.reach);
        // dict[pps.sourceName].uniqueReach += fixNumber(pps.)

        // Just get the impressions
        impressions.push({
          group: `${pps.sourceName}|${db.demoCategoryName}`,
          value: fixNumber(pps.impressionCount.count),
          date: new Date(ti.timeInterval.startTime),
        });

        Object.entries(pps.frequencyHistogram).forEach(([key, value]) => {
          frequencies.push({
            group: `${pps.sourceName}|${db.demoCategoryName}`,
            value,
            date: `${key}+`,
          });
        });
      })
      overview.totalImpressions += fixNumber(db.unionSource.impressionCount.count);
      overview.totalAverageFrequency += fixNumber(db.unionSource.frequencyHistogram[1]);
      overview.totalReach += fixNumber(db.unionSource.reach);
    });
  });

  // const individualPublishers = ['A', 'B', 'C']
  // dict['A'].uniqueReach = overview.totalReach - dict['BC'].reach
  // dict['B'].uniqueReach = overview.totalReach - dict['AC'].reach
  // dict['C'].uniqueReach = overview.totalReach - dict['AB'].reach

  return {
    impressions,
    frequencies,
    overview,
    summary: Object.values(dict),
  }
}

const handleUiReport = (report: Report|undefined): UiReport|null => {
  if (!report) {
    return null;
  }

  const {impressions, frequencies, summary, overview} = getImpressionsAndFrequencies(report)
  const {uniqueReach, totalReach} = getReaches(report);

  const res =  {
    id: report.reportId,
    name: report.name,
    status: report.state,
    overview,
    summary,
    impressions: filter(impressions, 'all'),
    uniqueReach: filter(uniqueReach, 'all'),
    totalReach: filter(totalReach, 'all'),
    averageFrequency: filter(frequencies, 'all'),
  };
  return res;
};

const filter = (data: ChartGroup[], filters: 'all'|string[]): ChartGroup[] => {
  // Remove data points that aren't contained in the filters
  // If 'all' then skip this step
  if (filters !== 'all') {

  } else {
    // ... do stuff ...
  }

  // Combine the remaining data points by date/publisher by summing the values
  // Gather dates and publishers
  const pubs = new Set<string>();
  const dates = new Set<string>();
  data.forEach(x => {
    pubs.add(x.group.slice(0, x.group.indexOf('|')));
    dates.add(x.date.toString());
  })

  const filteredResults: ChartGroup[] = [];
  for (let pub of pubs) {
    for (let date of dates) {
      const something = data.filter(x => x.group.startsWith(pub) && x.date.toString() === date);
      const va = something.reduce((a, b) => {
        return {
          ...b,
          value: a.value + b.value
        }
      })
      va.group = pub;
      filteredResults.push(va);
    }
  }

  return filteredResults
}

export const ReportViewModel = () => {
  const {loadReport} = ReportRepository();
  const [loading, setLoading] = useState<boolean>(false);
  const [report, setReport] = useState<UiReport|null>();
  const [errors, setErrors] = useState<string[]>([]);

  const load = async (id: string) => {
    setLoading(true);

    // Make all the calls
    const responses = await Promise.allSettled([
      loadReport(id)
    ]);
    const tempErrors:string[] = [];

    // Handle promises
    const response = responses[0];
    if(response.status === 'rejected') {
      tempErrors.push(response.reason);
      setErrors(errors);
    } else {
      const result = handleUiReport(response.value);
      setReport(result);
    }

    setLoading(false);
  };

  return {
    errors,
    loading,
    report,
    load,
  }
}
