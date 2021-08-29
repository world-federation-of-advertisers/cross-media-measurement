// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDuchyMeasurementLogEntry

class SpannerMeasurementLogEntriesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : MeasurementLogEntriesCoroutineImplBase() {
  override suspend fun createDuchyMeasurementLogEntry(
    request: CreateDuchyMeasurementLogEntryRequest
  ): DuchyMeasurementLogEntry {
    if (request.measurementLogEntryDetails.error.type ==
        MeasurementLogEntry.ErrorDetails.Type.PERMANENT
    ) {
      failGrpc(Status.INVALID_ARGUMENT) {
        "MeasurementLogEntries Service does not support PERMANENT errors, " +
          "use FailComputationParticipant instead."
      }
    }

    try {
      return CreateDuchyMeasurementLogEntry(request).execute(client, idGenerator, clock)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Measurement not found" }
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
      }
    }
  }
}
