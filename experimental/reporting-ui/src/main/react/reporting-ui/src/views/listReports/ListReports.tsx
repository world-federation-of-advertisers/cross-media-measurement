import React from 'react';
import {ReportsList} from 'ocmm-comp-lib/src/widgets';
import {columns} from './config';

export const ListReports = () => {
  return <ReportsList linkBase="reports/" columns={columns} />;
};
