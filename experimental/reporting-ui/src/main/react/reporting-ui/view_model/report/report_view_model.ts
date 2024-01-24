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
import { Metric } from '../../model/reporting';
import { ChartGroup } from '../../model/chart_group';

type UiReport = {
  id: string,
  name: string,
  status: string,
  overview: Overview,
  summary: SummaryPublisherData[],
  impressions: ChartGroup[],
  cumulativeImpressions: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  totalCumulativeReach: ChartGroup[],
  averageFrequency: ChartGroup[],
}

type ChartAndMetaData = {
  impressions: ChartGroup[],
  cumulativeImpressions: ChartGroup[],
  frequencies: ChartGroup[],
  uniqueReach: ChartGroup[],
  totalReach: ChartGroup[],
  totalCumulativeReach: ChartGroup[],
  summary: SummaryPublisherData[],
  overview: Overview,
}

const fixNumber = (num: number): number => {
  const newNum = Number(num);
  return Number.isNaN(newNum) || newNum < 0 ? 0 : newNum;
}

const setReach = (source: Metric, time: Date, demoCategory: string, reachArr: ChartGroup[]) => {
  const reach = {
    group: `${source.sourceName}|${demoCategory}`,
    value: fixNumber(source.reach),
    date: new Date(time),
  };
  reachArr.push(reach);
}

const setUniqueReach = (source: Metric, time: Date, demoCategory: string, reachArr: ChartGroup[]) => {
  reachArr.push({
    value: fixNumber(source.uniqueReach),
    group: `${source.sourceName}|${demoCategory}`,
    date: new Date(time),
  });
}

const getDefaultSummaryPublisherData = (sourceName: string, pubDataByPub: Map<string, SummaryPublisherData>) => {
  const summaryPublisherData = {
    id: sourceName,
    publisher: sourceName,
    impressions: 0,
    reach: 0,
    onTargetReach: 0,
    uniqueReach: 0,
    averageFrequency: 0,
  }
  pubDataByPub.set(sourceName, summaryPublisherData);
  return summaryPublisherData;
};

const setSummaryPublisherData = (source: Metric, pubDataByPub: Map<string, SummaryPublisherData>, lastDay: boolean) => {
  const summaryPubData = pubDataByPub.get(source.sourceName)
    || getDefaultSummaryPublisherData(source.sourceName, pubDataByPub);
  if (source.cumulative && lastDay) {
    // Take the frequencies from the last day.
    summaryPubData.impressions = fixNumber(source.impressionCount.count);
    summaryPubData.reach = fixNumber(source.reach);
    summaryPubData.uniqueReach = fixNumber(source.uniqueReach)
    summaryPubData.averageFrequency = summaryPubData.impressions / summaryPubData.reach
  }
  // No else, these have to be cumulative data.
}

const setImpressions = (source: Metric, time: Date, demoCategory: string, impArr: ChartGroup[]) => {
  const imp = {
    group: `${source.sourceName}|${demoCategory}`,
    value: fixNumber(source.impressionCount.count),
    date: new Date(time),
  };
  impArr.push(imp);
}

const setFrequencies = (source: Metric, demoCategory: string, freqArr: ChartGroup[], lastDay: boolean) => {
  if (source.cumulative && lastDay) {
    // Take the frequencies from the last day.
    Object.entries(source.frequencyHistogram).forEach(([key, value]) => {
      const frequency = {
        group: `${source.sourceName}|${demoCategory}`,
        value,
        date: `${key}+`
      };
      freqArr.push(frequency)
    });
  } else {
    // Add up the frequencies over every day.
    // TODO(@bdomen): Finish implementing
  }
}

const setOverview = (source: Metric, lastDay: boolean, overview: Overview) => {
  if(source.cumulative && lastDay) {
    // Take the values from the last day of the cumulative source.
    overview.totalImpressions = fixNumber(source.impressionCount.count);
    overview.totalReach = fixNumber(source.reach);
    overview.totalAverageFrequency = overview.totalImpressions / overview.totalReach;
  }
  // No else, must use the cumulative union
}

const getReportChartAndMeta = (report: Report): ChartAndMetaData => {
  const frequencies: ChartGroup[] = [];
  const impressions: ChartGroup[] = [];
  const cumulativeImpressions: ChartGroup[] = [];
  const uniqueReach: ChartGroup[] = [];
  const totalReach: ChartGroup[] = [];
  const totalCumulativeReach: ChartGroup[] = [];
  const overview: Overview = {
    totalImpressions: 0,
    totalReach: 0,
    totalAverageFrequency: 0,
    totalOnTargetReach: 0,
    totalUniqueReach: 0,
    // TODO: Get these from the report after changes are finished
    startDate: new Date(2024, 0, 1).toLocaleDateString(),
    endDate: new Date(2024, 0, 30).toLocaleDateString(),
  }

  // Don't add the complements or union
  const pubDatabyPub = new Map<string, SummaryPublisherData>();

  report.timeInterval.forEach((ti, intervalIndex) => {
    ti.demoBucket.forEach(db => {
      const lastDay = intervalIndex == report.timeInterval.length - 1;
      db.perPublisherSource.forEach(pps => {
        const reachArr = pps.cumulative ? totalCumulativeReach : totalReach;
        setReach(pps, ti.timeInterval.startTime, db.demoCategoryName, reachArr);
        setUniqueReach(pps, ti.timeInterval.startTime, db.demoCategoryName, uniqueReach)
        setSummaryPublisherData(pps, pubDatabyPub, lastDay);
        const impArr = pps.cumulative ? cumulativeImpressions : impressions;
        setImpressions(pps, ti.timeInterval.startTime, db.demoCategoryName, impArr);
        setFrequencies(pps, db.demoCategoryName, frequencies, lastDay);
      });

      db.unionSource.forEach(us => {
        const reachArr = us.cumulative ? totalCumulativeReach : totalReach;
        setReach(us, ti.timeInterval.startTime, db.demoCategoryName, reachArr);
        const impArr = us.cumulative ? cumulativeImpressions : impressions;
        setImpressions(us, ti.timeInterval.startTime, db.demoCategoryName, impArr);
        setFrequencies(us, db.demoCategoryName, frequencies, lastDay);
        setOverview(us, lastDay, overview);
      });
    });
  });

  return {
    impressions,
    cumulativeImpressions,
    frequencies,
    uniqueReach,
    totalReach,
    totalCumulativeReach,
    overview,
    summary: Array.from(pubDatabyPub.values()),
  }
}

const handleUiReport = (report: Report|undefined): UiReport|null => {
  if (!report) {
    return null;
  }

  const {
    impressions,
    cumulativeImpressions,
    frequencies,
    summary,
    overview,
    uniqueReach,
    totalReach,
    totalCumulativeReach,
  } = getReportChartAndMeta(report)

  const uiReport = {
    id: report.reportId,
    name: report.name,
    status: report.state,
    overview,
    summary,
    impressions: filter(impressions),
    cumulativeImpressions: filter(cumulativeImpressions),
    uniqueReach: filter(uniqueReach),
    totalReach: filter(totalReach),
    totalCumulativeReach: filter(totalCumulativeReach),
    averageFrequency: filter(frequencies),
  };

  return uiReport;
};

// TODO(@bdomen-ggl): Mostly stubbed. Finish implementing the filtering.
const filter = (data: ChartGroup[], filters: 'all'|string[] = 'all'): ChartGroup[] => {
  // Remove data points that aren't contained in the filters
  // If 'all' then skip this step
  if (filters !== 'all') {

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
