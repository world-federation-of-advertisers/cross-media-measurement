 /* Copyright 2023 The Cross-Media Measurement Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

import React, {useEffect} from 'react';
import { ListReportViewModel } from '../../view_model/report_list/report_list_view_model';
import { ReportListTable } from './component/table';
import { Loader } from '../../component/loader/loader';
import { Error } from '../../component/error/error';
import Card from 'react-bootstrap/esm/Card';
import { Header } from './component/header';

export const ReportListView = ({baseLink}) => {
  const { reports, errors, load, loading } = ListReportViewModel();

  useEffect(() => {
    load();
  }, []);

  if (errors.length > 0) {
    return <Error errorMessages={errors} />;
  }

  if (loading) {
    return <Loader />;
  }

  return(
    <React.Fragment>
      <Header />
      <Card className="SummaryCard">
        <Card.Body>
          <ReportListTable reports={reports} baseLink={baseLink}/>
        </Card.Body>
      </Card>
    </React.Fragment>
  );
}
