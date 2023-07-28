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
import {ReportApi} from './reportApi';
import {memoizePromiseFn} from './memoize';
import path from 'path';

export class RealApi implements ReportApi {
  // eslint-disable-next-line node/no-unsupported-features/node-builtins
  baseUrl: URL = new URL('');

  constructor() {}

  init(props: InitApiProps): void {
    this.baseUrl = props.endpoint;
  }

  async listReports(): Promise<ListReportsResponse> {
    return fetch(path.join(this.baseUrl.toString(), '/api/reports'))
      .then(res => res.json())
      .then(() => {
        // TODO: Translate result
        return {
          reports: [],
        };
      });
  }

  async getReport(req: GetReportRequest): Promise<GetReportResponse> {
    const getCached = memoizePromiseFn(this._getReport);

    return getCached(req);
  }

  async _getReport(req: GetReportRequest): Promise<GetReportResponse> {
    return fetch(path.join(this.baseUrl.toString(), '/api/reports', req.id))
      .then(res => res.json())
      .then(() => {
        // TODO: Translate result
        return {
          report: undefined,
        };
      });
  }

  async createReport(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
