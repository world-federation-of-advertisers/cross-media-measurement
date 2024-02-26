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
import { CardWrapper as Card } from '../../../component/card_wrapper/card_wrapper';
import { formatNumberWithMagnitude } from '../../../util/formatting';

export type ReportOverviewProps = {
  id: string,
  title: string,
  value: number|string,
}

export function ReportOverviewCard({id, title, value}: ReportOverviewProps) {
  if (typeof value === "number")  {
    return (
      <Card cardId={id} title={title} content={formatNumberWithMagnitude(value, 1)} />
    )
  } else {
    return (
      <Card cardId={id} title={title} content={value} />
    )
  }
}
