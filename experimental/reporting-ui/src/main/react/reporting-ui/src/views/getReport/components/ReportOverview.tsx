import React from 'react';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import './ReportOverview.css';
import {GetReportOverviews, ReportOverviewProps} from 'ocmm-comp-lib';

type ReportTotalsProps = {
  reportOverviews: ReportOverviewProps[];
};

export function ReportOverviewStats({reportOverviews}: ReportTotalsProps) {
  const reportOverviewElements = GetReportOverviews({props: reportOverviews});
  const objs = Object.entries(reportOverviewElements).map(([key, value]) => (
    <Col key={key}>{value}</Col>
  ));

  return <Row id="report-overview">{objs}</Row>;
}
