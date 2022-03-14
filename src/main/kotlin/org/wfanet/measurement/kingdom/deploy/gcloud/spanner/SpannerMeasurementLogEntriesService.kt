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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry.ErrorDetails.Type.TRANSIENT
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDuchyMeasurementLogEntry

class SpannerMeasurementLogEntriesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : MeasurementLogEntriesCoroutineImplBase() {
  override suspend fun createDuchyMeasurementLogEntry(
    request: CreateDuchyMeasurementLogEntryRequest
  ): DuchyMeasurementLogEntry {

    if (request.measurementLogEntryDetails.hasError()) {
      grpcRequire(request.measurementLogEntryDetails.error.type == TRANSIENT) {
        "MeasurementLogEntries Service only supports TRANSIENT errors, " +
          "use FailComputationParticipant instead."
      }
    }

    try {
      return CreateDuchyMeasurementLogEntry(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.MEASUREMENT_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Measurement not found" }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.CERTIFICATE_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }
}
