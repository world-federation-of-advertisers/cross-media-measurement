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
<<<<<<< HEAD
import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
<<<<<<<< HEAD:src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/LegacyDataServices.kt
import org.wfanet.measurement.kingdom.db.ReportDatabase
import org.wfanet.measurement.kingdom.db.RequisitionDatabase

<<<<<<< HEAD:src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/LegacyDataServices.kt
<<<<<<< HEAD:src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/LegacyDataServices.kt
/** Builds a list of all the Kingdom's legacy internal data-layer services. */
=======
/** Builds a list of all the Kingdom's internal data-layer services. */
>>>>>>> bbcf20ac (first commit):src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/DataServices.kt
=======
/** Builds a list of all the Kingdom's legacy internal data-layer services. */
>>>>>>> ed9def1e (halfway there):src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/DataServices.kt
=======
import org.wfanet.measurement.kingdom.db.ReportDatabase
import org.wfanet.measurement.kingdom.db.RequisitionDatabase

/** Builds a list of all the Kingdom's legacy internal data-layer services. */
>>>>>>> 382125ff (fixed build and lint more)
fun buildLegacyDataServices(
  reportDatabase: ReportDatabase,
  requisitionDatabase: RequisitionDatabase
): List<BindableService> {
  return listOf(
    ReportsService(reportDatabase),
    ReportLogEntriesService(reportDatabase),
    RequisitionsService(requisitionDatabase)
  )
}
<<<<<<< HEAD
========
>>>>>>>> 382125ff (fixed build and lint more):src/main/kotlin/org/wfanet/measurement/kingdom/service/internal/DataServices.kt

/** Builds a list of all the Kingdom's internal data-layer services. */
fun buildDataServices(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
): List<BindableService> {
  return listOf(
    SpannerCertificatesService(clock, idGenerator, client),
    SpannerDataProvidersService(clock, idGenerator, client),
    SpannerEventGroupsService(clock, idGenerator, client),
    SpannerMeasurementConsumersService(clock, idGenerator, client),
    SpannerMeasurementsService(clock, idGenerator, client),
    SpannerRequisitionsService(clock, idGenerator, client)
  )
}
=======
>>>>>>> 382125ff (fixed build and lint more)
