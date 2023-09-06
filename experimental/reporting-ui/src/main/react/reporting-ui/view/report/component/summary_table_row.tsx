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
import { SummaryPublisherData } from '../../../model/reporting';
import { PublisherCell } from './publisher_cell';
import { numberWithMagnitude } from '../../../util/numberWithMagnitude';

type ReportSummaryProps = {
  pubData: SummaryPublisherData,
  color: string,
};

export function SummaryTableRow({
  pubData,
  color,
}: ReportSummaryProps) {
  return (
    <tr>
      <td>
        <PublisherCell
          publisherName={pubData.publisher}
          publisherColor={color}
        />
      </td>
      <td>{numberWithMagnitude(pubData.impressions, 1)}</td>
      <td>{numberWithMagnitude(pubData.reach, 1)}</td>
      <td>{numberWithMagnitude(pubData.onTargetReach, 1)}</td>
      <td>{numberWithMagnitude(pubData.uniqueReach, 1)}</td>
      <td>{numberWithMagnitude(pubData.averageFrequency, 1)}</td>
    </tr>
  )
}
