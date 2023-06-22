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
import { Link } from 'react-router-dom';
import { Report } from '../../models';

// utility map of Report properties to UI elements.
export const columnsRenderer = {
    'name': (report: Report, baseLink: string) => <Link to={baseLink + report.id}>{report.name}</Link>,
    'status': (report: Report) => <div>{report.status}</div>,
}

// given a dictionary of columns {property, display}, generate table headers.
export const processHeaders = (columns: {[ id:string] : string}) => {
    const headers = Object.entries(columns).map(([key, value]) => <th key={key}>{value}</th>);
    return (
        <tr>
            {headers}
        </tr>
    );
}

export const processAllColumns = (reports: Report[], linkBase: string, columns: {[ id:string] : string}) => {
    const reportsColumns = reports.map((x: Report) =>
        processColumns(x, linkBase, columns)
    )
    return reportsColumns;
}

// given a dictionary of column defs and a report, generate table cells.
// some properties can have special processing like "name" which creates a link
const processColumns = (report: Report, baseLink: string, columns: {[ id:string] : string}) => {
    const cols = Object.entries(columns).map(([key]) => {
        if(key === 'name') {
            return <td key={key}>{columnsRenderer[key](report, baseLink)}</td>
        } else {
            return <td key={key}>{columnsRenderer[key](report)}</td>
        }
    });

    return (
        <tr key={`get-report-list-${report.id}`}>
            {cols}
        </tr>
    )
}
