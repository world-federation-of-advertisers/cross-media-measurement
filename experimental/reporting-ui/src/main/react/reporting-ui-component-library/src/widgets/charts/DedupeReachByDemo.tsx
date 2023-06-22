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
import { GetDedupeReachByDemoResolver } from '../../utils/resolvers';
import { ChartFactory, chartType } from '../components/chartHelpers/ChartFactory';
import { chartProps } from './ChartProps';
import { Demo } from '../../models';

export function DedupeReachByDemo({id, reportId, config}: chartProps) {
    const [ data, setData ] = useState<Demo[] | undefined>();

    useEffect(() => {
        GetDedupeReachByDemoResolver(reportId).then(setData);
    }, [reportId]);

    if (!data) {
        return null;
    }

    return (
        <ChartFactory
            cardId={id}
            title='De-duplicated reach by demographic'
            data={data}
            config={config}
            type={chartType.bar}
        />
    )
}
