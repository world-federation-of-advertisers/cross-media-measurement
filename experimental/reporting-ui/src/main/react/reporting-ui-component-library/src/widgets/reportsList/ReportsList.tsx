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
import Table from 'react-bootstrap/Table';
import FakeApi from '../../utils/api/fakeApi';
import { processAllColumns, processHeaders } from './utils';

type ReportsListProps = {
    linkBase: string, // Base path to appened to the link, include trailing slash
    columns: {[ id:string] : string},
}

export function  ReportsList({linkBase, columns}: ReportsListProps) {
    const [reportsList, setReportsList] = useState<JSX.Element[] | null>(null);

    useEffect(() => {
        FakeApi.listReports().then(response => {
            const reports = processAllColumns(response.reports, linkBase, columns);
            setReportsList(reports);
         });
    }, []);

    return (
        <Table striped bordered hover>
            <thead>
                {processHeaders(columns)}
            </thead>
            <tbody>
                {reportsList}
            </tbody>
        </Table>
    )
}
