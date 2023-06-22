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

import React, {useEffect, useState} from 'react';
import Stack from 'react-bootstrap/Stack';
import Table from 'react-bootstrap/Table';
import Card from 'react-bootstrap/Card';
import {GetReportSummariesResolver} from '../utils/resolvers';
import {SquareIcon} from '../assets/icons';
import {SummaryPublisherData} from '../models';
import {numberWithMagnitude} from '../utils/helpers/numberToMagnitudeString';
import './ReportSummary.css';

type TableConfig = {
  columns: {[id: string]: string};
};

type ReportSummaryProps = {
  reportId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  config: any;
  tableConfig: TableConfig;
};

export function ReportSummary({
  reportId,
  config,
  tableConfig,
}: ReportSummaryProps) {
  const [reportSummaries, setReportSummaries] = useState<
    SummaryPublisherData[] | undefined
  >(undefined);

  useEffect(() => {
    GetReportSummariesResolver(reportId).then(setReportSummaries);
  }, [reportId]);

  if (!reportSummaries) {
    return null;
  }

  const summaries = reportSummaries.map((report: SummaryPublisherData) => {
    const cols = Object.entries(tableConfig.columns).map(([key]) => {
      if (key === 'publisher') {
        return (
          <td key={`${report.id}-${key}`}>
            <Stack direction="horizontal" gap={2}>
              <SquareIcon fill={config.pubColors[report.id]} />
              {report.publisher}
            </Stack>
          </td>
        );
      } else {
        return (
          <td key={`${report.id}-${key}`}>
            {numberWithMagnitude(report[key], 1)}
          </td>
        );
      }
    });
    return <tr key={report.id}>{cols}</tr>;
  });

  const colHeaders = Object.entries(tableConfig.columns).map(([key, value]) => (
    <th key={key}>{value}</th>
  ));

  return (
    <Card className="SummaryCard">
      <Card.Body>
        <Table hover>
          <thead>
            <tr>{colHeaders}</tr>
          </thead>
          <tbody>{summaries}</tbody>
        </Table>
      </Card.Body>
    </Card>
  );
}
