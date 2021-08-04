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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurement

class SpannerMeasurementsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : MeasurementsCoroutineImplBase() {
  override suspend fun createMeasurement(request: Measurement): Measurement {
    try {
      return CreateMeasurement(request).execute(client, idGenerator, clock)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "MeasurementConsumer not found" }
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "DataProvider not found" }
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS -> throw e
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "Duchy not found" }
      }
    }
  }
  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    // return MeasurementReader()
    //   .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalEventGroupId))
    //   ?.measurement
    //   ?: failGrpc(Status.NOT_FOUND) { "Measurement not found" }
    TODO("later")
  }
  override suspend fun getMeasurementByComputationId(
    request: GetMeasurementByComputationIdRequest
  ): Measurement {
    return MeasurementReader()
      .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalComputationId))
      ?.measurement
      ?: failGrpc(Status.NOT_FOUND) { "Measurement not found" }
  }
}
