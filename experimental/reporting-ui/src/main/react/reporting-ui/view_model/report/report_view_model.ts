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
    console.log('TI', ti)
    ti.demoBucket.forEach(db => {
      const fullUnion = db.unionSource;
      db.perPublisherSource.forEach(pub => {
        totalReach.push({
          value: pub.reach,
          group: `${pub.sourceName}-${db.demoCategoryName}`,
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

  const dict: { [id: string] : SummaryPublisherData; }= {};

  report.timeInterval.forEach(ti => {
    ti.demoBucket.forEach(db => {
      db.perPublisherSource.forEach(pps => {
        if (Object.keys(dict).includes(pps.sourceName)) {
          dict[pps.sourceName].impressions += pps.impressions;
          dict[pps.sourceName].averageFrequency += pps.frequency;
          dict[pps.sourceName].reach += pps.reach;

          // Just get the impressions
          impressions.push({
            group: `${pps.sourceName}-${db.demoCategoryName}`,
            value: pps.impressions,
            date: new Date(ti.timeInterval.startTime),
          });

          // Get all for now, process later??
          frequencies.push({
            group: `${pps.sourceName}-${db.demoCategoryName}`,
            value: pps.frequency,
            date: new Date(ti.timeInterval.startTime),
          });
        }
      })
      // overview.totalImpressions += db.unionSource.impressions;
      // overview.totalAverageFrequency += db.unionSource.frequency;
      // overview.totalReach += db.unionSource.reach;
    });
  });

  const individualPublishers = ['A', 'B', 'C']
  // dict['A'].uniqueReach = overview.totalReach - dict['BC'].reach
  // dict['B'].uniqueReach = overview.totalReach - dict['AC'].reach
  // dict['C'].uniqueReach = overview.totalReach - dict['AB'].reach

  return {
    impressions,
    frequencies,
    overview,
    summary: individualPublishers.map(x => dict[x]),
  }
}

const handleUiReport = (report: Report|undefined): UiReport|null => {
  if (!report) {
    return null;
  }

  // const {impressions, frequencies, summary, overview} = getImpressionsAndFrequencies(report)
  const {uniqueReach, totalReach} = getReaches(report);

  const res =  {
    id: report.reportId,
    name: report.name,
    status: report.state,
    overview: {
      totalImpressions: 0,
      totalReach: 0,
      totalOnTargetReach: 0,
      totalUniqueReach: 0,
      totalAverageFrequency: 0,
    },
    summary:[],
    impressions:[],
    uniqueReach:[],
    totalReach,
    averageFrequency:[],
  };
  console.log(res)
  return res;
};

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
