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

import {
  GetReportRequest,
  GetReportResponse,
  InitApiProps,
  ListReportsResponse,
} from './models';
import {ReportingClient} from './reporting_client';

export class RealApi implements ReportingClient {
  // eslint-disable-next-line node/no-unsupported-features/node-builtins
  baseUrl: URL;

  constructor(props: InitApiProps) {
    this.baseUrl = props.endpoint;
  }

  async listReports(): Promise<ListReportsResponse> {
    const res = await fetch(this.baseUrl.toString() + '/api/reports');
    const reports = await res.json();
    const response = Object.freeze({
      reports,
    });
    return response;
  }

  async getReport(req: GetReportRequest): Promise<GetReportResponse> {
    const res = await fetch(this.baseUrl.toString() + '/api/reports' + req.id);
    const report = await res.json();
    const response = Object.freeze({
      report,
    });
    return response;
  }
}
