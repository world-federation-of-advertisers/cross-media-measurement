import {useState} from 'react';
import { Report, ReportState } from '../../model/reporting';
import { ShowReportRepository } from '../../model/show_report/show_report_repository';

type ShowReport = {
  id: string,
  name: string,
  status: ReportState,
}

const handleShowReport = (report: Report) => {
  return {
    id: report.id,
    name: report.name,
    status: report.status,
  };
};

export const ShowReportViewModel = () => {
  const {loadReport} = ShowReportRepository();
  const [loading, setLoading] = useState<boolean>(false);
  const [report, setReport] = useState<ShowReport|null>();
  const [errors, setErrors] = useState<string[]>([]);

  const load = async (id: string) => {
    setLoading(true);

    // Make all the calls
    const responses = await Promise.allSettled([
      loadReport(id)
    ]);
    const tempErrors:string[] = [];

    // Handle promises
    const response = responses[0];
    if(response.status === 'rejected') {
      tempErrors.push(response.reason);
      setErrors(errors);
    } else {
      if (response.value) {
        const result = handleShowReport(response.value);
        setReport(result);
      } else {
        setReport(null);
      }
    }

    setLoading(false);
  };

  return {
    errors,
    loading,
    report,
    load,
  }
}