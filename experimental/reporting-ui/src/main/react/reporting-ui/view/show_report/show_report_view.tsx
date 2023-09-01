import React, {useEffect} from 'react';
import { useParams } from 'react-router-dom';
import { ShowReportViewModel } from '../../view_model/show_report/show_report_view_model';
import { Loader } from '../../component/loader/loader';
import { Error } from '../../component/error/error';
import { TERMINAL_STATES } from '../../model/reporting';

export const ShowReportView = () => {
  const { errors, report, load, loading } = ShowReportViewModel();
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
    return <div>{report.name}</div>
  }

  return(
    <div>Hello NonTerminal</div>
  );
}
