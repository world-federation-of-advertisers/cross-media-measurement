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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryError
import org.wfanet.measurement.internal.kingdom.StateTransitionMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.StreamStateTransitionMeasurementLogEntriesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamStateTransitionMeasurementLogEntries
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDuchyMeasurementLogEntry

class SpannerMeasurementLogEntriesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementLogEntriesCoroutineImplBase(coroutineContext) {
  override suspend fun createDuchyMeasurementLogEntry(
    request: CreateDuchyMeasurementLogEntryRequest
  ): DuchyMeasurementLogEntry {

    if (request.measurementLogEntryDetails.hasError()) {
      grpcRequire(
        request.measurementLogEntryDetails.error.type == MeasurementLogEntryError.Type.TRANSIENT
      ) {
        "MeasurementLogEntries Service only supports TRANSIENT errors, " +
          "use FailComputationParticipant instead."
      }
    }

    try {
      return CreateDuchyMeasurementLogEntry(request).execute(client, idGenerator)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement in wrong state. state=${e.state}",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error")
    }
  }

  override fun streamStateTransitionMeasurementLogEntries(
    request: StreamStateTransitionMeasurementLogEntriesRequest
  ): Flow<StateTransitionMeasurementLogEntry> {
    return StreamStateTransitionMeasurementLogEntries(
        ExternalId(request.externalMeasurementId),
        ExternalId(request.externalMeasurementConsumerId),
      )
      .execute(client.singleUse())
      .map { it.stateTransitionMeasurementLogEntry }
  }
}
