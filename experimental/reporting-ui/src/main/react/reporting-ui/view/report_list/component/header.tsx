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
import Navbar from 'react-bootstrap/Navbar';
import {
  AddIcon,
  DownloadIcon,
  FilterIcon,
  MenuIcon,
  TrailingIcon,
} from '../../../public/asset/icon';
import './header.css';

type HeaderProps = {
};

export function Header({}: HeaderProps) {
  return (
    <React.Fragment>
      <Navbar id="get-report-header" className="bg-body-tertiary">
        <div id="get-report-header-menu">
          <MenuIcon />
        </div>
        <Navbar.Brand id="header-title">Reporting UI Demo - Halo</Navbar.Brand>
      </Navbar>
    </React.Fragment>
  );
}
