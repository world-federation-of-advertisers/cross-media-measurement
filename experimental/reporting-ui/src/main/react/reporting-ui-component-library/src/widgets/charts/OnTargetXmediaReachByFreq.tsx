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
import { GetTargetXmediaReachByFreqResolver } from '../../utils/resolvers';
import { ChartFactory, chartType } from '../components/chartHelpers/ChartFactory';
import { chartProps } from './ChartProps';
import { TargetReach } from '../../models';

const neutralColors = [
    "#CACACA",
    "#959595",
    "#5E5E5E",
  ]

export function OnTargetXmediaReachByFreq({id, reportId, config}: chartProps) {
    const [ data, setData ] = useState<TargetReach[] | undefined>();

    useEffect(() => {
        GetTargetXmediaReachByFreqResolver(reportId, [1, 2, 3]).then(reachData => {
            const colors = {}
            const unique = [...new Set(reachData?.map(item => item.cat))];
            unique.forEach((x: any, i) => colors[x] = neutralColors[i])
            config.catColors = colors;
            setData(reachData)
        });
    }, [reportId]);

    if (!data) {
        return null;
    }

    return (
        <ChartFactory
            cardId={id}
            title='On target cross-media reach by frequency'
            data={data}
            config={config}
            type={chartType.percentMultiLine}
        />
    )
}
