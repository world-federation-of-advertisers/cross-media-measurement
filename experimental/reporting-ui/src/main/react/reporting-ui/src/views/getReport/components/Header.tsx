import React, {useEffect, useState} from 'react';
import Navbar from 'react-bootstrap/Navbar';
import {
  AddIcon,
  DownloadIcon,
  FilterIcon,
  MenuIcon,
  TrailingIcon,
} from '../../../assets/icons';
import {GetReportNameResolver} from 'ocmm-comp-lib';
import './Header.css';

type GetReportHeaderProps = {
  reportId: string;
};

export function GetReportHeader({reportId}: GetReportHeaderProps) {
  const [reportName, setReportName] = useState();

  useEffect(() => {
    GetReportNameResolver(reportId).then(r => setReportName(r));
  }, [reportId]);

  return (
    <React.Fragment>
      <Navbar id="get-report-header" className="bg-body-tertiary">
        <div id="get-report-header-menu">
          <MenuIcon />
        </div>
        <Navbar.Brand id="header-title">{reportName}</Navbar.Brand>
        <Navbar.Toggle />
        <Navbar.Collapse id="header-collapse" className="justify-content-end">
          <TrailingIcon />
          <FilterIcon />
          <AddIcon />
          <DownloadIcon />
        </Navbar.Collapse>
      </Navbar>
    </React.Fragment>
  );
}
