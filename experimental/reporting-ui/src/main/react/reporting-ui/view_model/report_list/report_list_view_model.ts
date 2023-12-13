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

import { useState } from 'react';
import { ReportListRepository } from '../../model/report_list/report_list_repository';
import { Report } from '../../model/reporting';

type ReportListItem = {
  id: string,
  name: string,
  status: string,
}

const handleLoadReports = (reports: Report[]) => {
  const uiReports = reports.map(apiReport => {
    return {
      id: apiReport.reportId,
      name: apiReport.name,
      status: apiReport.state,
    }
  });

  return uiReports;
}

export const ListReportViewModel = () => {
  const {loadReports} = ReportListRepository();
  const [loading, setLoading] = useState<boolean>(false);
  const [reports, setReports] = useState<ReportListItem[]>([]);
  const [errors, setErrors] = useState<string[]>([]);

  const load = async () => {
    setLoading(true);

    // Make all the calls
    const responses = await Promise.allSettled([
      loadReports()
    ]);
    const tempErrors:string[] = [];

    // Handle promises
    const response = responses[0];
    if(response.status === 'rejected') {
      tempErrors.push(response.reason);
      setErrors(errors);
    } else {
      const result = handleLoadReports(response.value);
      setReports(result);
    }

    setLoading(false);
  }

  return {
    errors,
    loading,
    reports,
    load,
  }
}
