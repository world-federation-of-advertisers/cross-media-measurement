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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ScheduleReader


abstract class KingdomDatabaseTestBase : UsingSpannerEmulator(KINGDOM_SCHEMA) {
  private suspend fun write(mutation: Mutation) = databaseClient.write(mutation)

  protected suspend fun insertMeasurementConsumer(
    measurementConsumerId: Long,
    externalMeasurementConsumerId: Long
  ) {
    write(
      Mutation.newInsertBuilder("MeasurementConsumers")
        .set("MeasurementConsumerId")
        .to(measurementConsumerId)
        .set("ExternalMeasurementConsumerId")
        .to(externalMeasurementConsumerId)
        .set("MeasurementConsumerDetails")
        .to(ByteArray.copyFrom(""))
        .set("MeasurementConsumerDetailsJson")
        .to("irrelevant-measurement-consumer-details-json")
        .build()
    )
  }
}
