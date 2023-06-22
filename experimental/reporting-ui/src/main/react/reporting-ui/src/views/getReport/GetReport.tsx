import React, {useEffect, useState} from 'react';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import {useParams} from 'react-router-dom';

import {ReportOverviewStats} from './components/ReportOverview';
import {GetReportHeader} from './components/Header';

import {ReportSummary} from 'ocmm-comp-lib';
import {
  DedupeReachByDemo,
  OnTargetReach,
  OnTargetUniqueReqchByPlat,
  TotalReach,
  UniqueReqchByPlat,
  XmediaReachByFreq,
  OnTargetXmediaReachByFreq,
  GetReportSummariesResolver,
  SummaryPublisherData,
} from 'ocmm-comp-lib';
import './GetReport.css';

const COLOR_CODES = ['#5E5E5E', '#FFA300', '#EA5F94', '#9D02D7'];

const tableConfig = {
  columns: {
    publisher: 'Publisher',
    impressions: 'Impressions',
    reach: 'Reach',
    onTargetReach: 'On target reach',
    uniqueReach: 'Unique reach',
    averageFrequency: 'Average frequency',
  },
};

const reportOverviews = reportId => [
  {
    id: 'overview-impressions',
    reportId,
    title: 'Impressions',
    prop: 'totalImpressions',
  },
  {
    id: 'overview-reach',
    reportId,
    title: 'Reach',
    prop: 'totalReach',
  },
  {
    id: 'overview-reach-target',
    reportId,
    title: 'On Reach Target',
    prop: 'totalOnTargetReach',
  },
  {
    id: 'overview-avg-freq',
    reportId,
    title: 'Average Frequency',
    prop: 'totalAverageFrequency',
  },
];

export const GetReport = () => {
  const {reportId} = useParams();
  const [reportSummaries, setReportSummaries] =
    useState<SummaryPublisherData[]>();

  if (!reportId) {
    return null;
  }

  useEffect(() => {
    GetReportSummariesResolver(reportId).then(setReportSummaries);
  }, [reportId]);

  if (!reportSummaries) {
    return null;
  }

  let i = 0;
  const pubColors = {
    [i++]: COLOR_CODES[0],
  };
  reportSummaries.forEach(x => (pubColors[x.id] = COLOR_CODES[i++]));

  const globalConfig = {
    pubColors,
    catColors: {}, // To be defined by specific widgets... guess it's not global then
  };

  return (
    <React.Fragment>
      <GetReportHeader reportId={reportId} />
      <ReportOverviewStats reportOverviews={reportOverviews(reportId)} />
      <ReportSummary
        reportId={reportId}
        config={globalConfig}
        tableConfig={tableConfig}
      />

      <Row className="report-charts">
        <Col className="report-chart">
          <OnTargetReach id={'id1'} reportId={reportId} config={globalConfig} />
        </Col>
        <Col className="report-chart">
          <TotalReach id={'id2'} reportId={reportId} config={globalConfig} />
        </Col>
      </Row>
      <Row className="report-charts">
        <Col className="report-chart">
          <OnTargetXmediaReachByFreq
            id={'id3'}
            reportId={reportId}
            config={globalConfig}
          />
        </Col>
        <Col className="report-chart">
          <XmediaReachByFreq
            id={'id4'}
            reportId={reportId}
            config={globalConfig}
          />
        </Col>
      </Row>
      <Row className="report-charts">
        <Col className="report-chart">
          <DedupeReachByDemo
            id={'id5'}
            reportId={reportId}
            config={globalConfig}
          />
        </Col>
      </Row>
      <Row className="report-charts">
        <Col className="report-chart">
          <OnTargetUniqueReqchByPlat
            id={'id6'}
            reportId={reportId}
            config={globalConfig}
          />
        </Col>
        <Col className="report-chart">
          <UniqueReqchByPlat
            id={'id7'}
            reportId={reportId}
            config={globalConfig}
          />
        </Col>
      </Row>
    </React.Fragment>
  );
};
