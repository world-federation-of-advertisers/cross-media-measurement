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
import { ChartGroup } from '../../../view_model/report/report_view_model';

type FrequenciesProps = {
    id: string,
    frequencies: ChartGroup[],
    pubColors: { [Name: string]: string},
}

export function Frequencies({id, frequencies, pubColors}: FrequenciesProps) {
    const config = {
        pubColors,
    }
    console.log(frequencies)
    return (
        <Chart
            cardId={id}
            title='Average Frequency'
            data={frequencies}
            config={config}
            type={ChartType.bar}
        />
    )
}
