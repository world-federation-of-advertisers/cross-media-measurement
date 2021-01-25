// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal

import io.grpc.BindableService
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

/** Builds a list of all the Kingdom's internal data-layer services. */
fun buildDataServices(relationalDatabase: KingdomRelationalDatabase): List<BindableService> {
  return listOf(
    ReportConfigSchedulesService(relationalDatabase),
    ReportConfigsService(relationalDatabase),
    ReportsService(relationalDatabase),
    ReportLogEntriesService(relationalDatabase),
    RequisitionsService(relationalDatabase)
  )
}
