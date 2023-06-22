import React, { useEffect, useState } from 'react';
import { CardWrapper as Card } from './components/CardWrapper';
import { GetReportOverviewResolver } from '../utils/resolvers/GetReportOverview';
import { Overview } from '../models';
import { numberWithMagnitude } from '../utils/helpers/numberToMagnitudeString';

export type ReportOverviewProps = {
  reportId: string,
  id: string // desired CSS id
  title: string, // title of the card
  prop: string, // name of the property on ReportOverview
}

export function ReportOverview({reportId, id, title, prop}: ReportOverviewProps) {
  const [reportOverview, setReportOverview] = useState<Overview | undefined>();

  useEffect(() => {
    GetReportOverviewResolver(reportId).then(setReportOverview);
  }, [reportId]);

  if (!reportOverview) {
    return null;
  }

  return (
    <Card cardId={id} title={title} content={numberWithMagnitude(reportOverview[prop], 1)} />
  )
}
