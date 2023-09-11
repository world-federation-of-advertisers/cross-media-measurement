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
import { ChartFactory, ChartType } from './chart_helper/chart_factory';
import { Reach } from '../../model/reporting';

type OnTargetReachProps = {
    id: string,
    reach: Reach[],
    pubColors: { [Name: string]: string}
}

export function OnTargetReach({id, reach, pubColors}: OnTargetReachProps) {
    const config = {
        pubColors,
    }
    return (
        <ChartFactory
            cardId={id}
            title='On target reach'
            data={reach}
            config={config}
            type={ChartType.multiLine}
        />
    )
}
