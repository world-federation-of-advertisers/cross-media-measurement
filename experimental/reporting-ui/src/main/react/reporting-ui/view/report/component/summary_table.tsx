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
import Table from 'react-bootstrap/Table';
import Card from 'react-bootstrap/Card';
import {SummaryPublisherData} from '../../../model/reporting';
import { SummaryTableRow } from './summary_table_row';
import './summary_table.css';

type PublisherConfig = {[id: string]: string};

type SummaryTableProps = {
  reportSummaries: SummaryPublisherData[];
  publisherColors: PublisherConfig;
};

export function SummaryTable({
  reportSummaries,
  publisherColors,
}: SummaryTableProps) {
  const summaries = reportSummaries.map((pubData: SummaryPublisherData) => {
    return(
      <SummaryTableRow key={'report-summary-table-' + pubData.id} pubData={pubData} color={publisherColors[pubData.id]} />
    )
  });

  return (
    <Card className="SummaryCard">
      <Card.Body>
        <Table hover>
          <thead>
            <tr>
              <td>Publisher</td>
              <td>Impressions</td>
              <td>Reach</td>
              <td>Unique reach</td>
              <td>Average frequency</td>
            </tr>
          </thead>
          <tbody>{summaries}</tbody>
        </Table>
      </Card.Body>
    </Card>
  );
}
