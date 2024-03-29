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

import ReactDOM from 'react-dom/client';
import AppConfig from '../../initialize';
import { FakeReportingClient } from './fake_reporting_client';
import 'bootstrap/dist/css/bootstrap.min.css';
import '../../../index.css';
import App from "../../../app";

const config = {
  reportingClient: new FakeReportingClient(),
};

AppConfig.initialize(config);

const root = ReactDOM.createRoot(document.getElementById('root')!);
root.render(<App />);
