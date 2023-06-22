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

import React, { useEffect, useState } from 'react';
import { ChartFactory, chartType } from '../components/chartHelpers/ChartFactory';
import { GetUniquReachByPlat } from '../../utils/resolvers';
import { chartProps } from './ChartProps';
import { UniqueReach } from '../../models';

export function UniqueReqchByPlat({id, reportId, config}: chartProps) {
    const [ data, setData ] = useState<UniqueReach[] | undefined>();

    useEffect(() => {
        GetUniquReachByPlat(reportId).then(setData);
    }, [reportId]);

    if (!data) {
        return null;
    }

    return (
        <ChartFactory
            cardId={id}
            title='Unique reach by platform'
            data={data}
            config={config}
            type={chartType.multiLine}
        />
    )
}
