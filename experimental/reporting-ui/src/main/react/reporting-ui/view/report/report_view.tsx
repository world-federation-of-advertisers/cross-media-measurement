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

import React, {useEffect} from 'react';
import { useParams } from 'react-router-dom';
import { ReportViewModel } from '../../view_model/report/report_view_model';
import { Loader } from '../../component/loader/loader';
import { Error } from '../../component/error/error';
import { TERMINAL_STATES } from '../../model/reporting';
import { TerminalReport } from './component/terminal_report';
import './report_view.css';

export const ReportView = () => {
  const { errors, report, load, loading } = ReportViewModel();
  const {reportId} = useParams();

  useEffect(() => {
    load(reportId!);
  }, [reportId])

  if (errors.length > 0) {
    return <Error errorMessages={errors} />;
  }

  if (loading) {
    return <Loader />;
  }

  if (!report) {
    return <div>Couldn't load Report</div>
  }

  if (TERMINAL_STATES.includes(report.status)) {
    return <TerminalReport
              name={report.name}
              overview={report.overview}
              summaries={report.summary}
              targetReach={report.targetReach}
              totalReach={report.totalReach}
              xmediaReach={report.xmediaReach}
              onTargetReach={report.onTargetReach}
              onTargetUniqueReach={report.onTargetUniqueReach}
              uniqueReachByPlat={report.uniqueReachByPlat}
              demo={report.demo}
            />
  }

  return(
    <div>{report.name}</div>
  );
}
