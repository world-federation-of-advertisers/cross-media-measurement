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
import reportWebVitals from './report_web_vitals';
import AppConfig from './client/initialize';
import { ReportingClientImpl } from './client/reporting/client_impl';
import App from "./app";
import './index.css';
import 'bootstrap/dist/css/bootstrap.min.css';

// TODO(@bdomen-ggl): Look into making this configurable.
// https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/4877bb901568597cf3d0ef8030e1b7e82e074803/src/test/kotlin/org/wfanet/measurement/integration/k8s/BUILD.bazel#L88C1-L88C1
const configProps = {
  reportingClient: new ReportingClientImpl(
    {
      endpoint: new URL('http://localhost:8080'),
      measurementConsumer: 'VCTqwV_vFXw',
    }),
};

AppConfig.initialize(configProps);

const root = ReactDOM.createRoot(document.getElementById('root')!);
root.render(<App />);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals(null);
