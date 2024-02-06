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
import { Chart, ChartType } from '../chart';
import { ChartGroup } from '../../../model/chart_group';

const neutralColors = [
    "#CACACA",
    "#959595",
    "#5E5E5E",
]

type XmediaReachByFreqProps = {
    id: string,
    reach: ChartGroup[],
}

export function XmediaReachByFreq({id, reach}: XmediaReachByFreqProps) {
    const config = {
        catColors: {}
    }
    const colors = {}
    const unique = [...new Set(reach?.map(item => item.group))];
    unique.forEach((x: any, i) => colors[x] = neutralColors[i])
    config.catColors = colors;
    return (
        <Chart
            cardId={id}
            title='Cross-media reach by frequency'
            data={reach}
            config={config}
            type={ChartType.percentMultiLine}
        />
    )
}
