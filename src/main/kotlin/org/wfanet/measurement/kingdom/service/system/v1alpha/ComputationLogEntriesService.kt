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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import io.grpc.Status
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest

class ComputationLogEntriesService(
  private val measurementLogEntriesService: MeasurementLogEntriesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
) : ComputationLogEntriesCoroutineImplBase(coroutineContext) {
  override suspend fun createComputationLogEntry(
    request: CreateComputationLogEntryRequest
  ): ComputationLogEntry =
    ReportTracing.traceSuspending(
      spanName = "kingdom.computation_log_entry.create",
      attributes = logEntryTraceAttributes(request.parent),
    ) {
      val computationParticipantKey =
        grpcRequireNotNull(ComputationParticipantKey.fromName(request.parent)) {
          "Resource name unspecified or invalid."
        }
      grpcRequire(request.hasComputationLogEntry()) { "computation_log_entry is missing." }
      if (computationParticipantKey.duchyId != duchyIdentityProvider().id) {
        failGrpc(Status.PERMISSION_DENIED) {
          "The caller identity doesn't match the specified log entry parent."
        }
      }
      val computationLogEntry = request.computationLogEntry
      val internalRequest =
        CreateDuchyMeasurementLogEntryRequest.newBuilder()
          .apply {
            externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
            externalDuchyId = computationParticipantKey.duchyId
            measurementLogEntryDetailsBuilder.apply {
              logMessage = computationLogEntry.logMessage
              if (computationLogEntry.hasErrorDetails()) {
                grpcRequire(
                  computationLogEntry.errorDetails.type ==
                    ComputationLogEntry.ErrorDetails.Type.TRANSIENT
                ) {
                  "Only transient error is support in the computationLogEntriesService."
                }
                error = computationLogEntry.errorDetails.toInternalLogErrorDetails()
              }
            }
            detailsBuilder.apply {
              duchyChildReferenceId = computationLogEntry.participantChildReferenceId
              if (computationLogEntry.hasStageAttempt()) {
                stageAttempt = computationLogEntry.stageAttempt.toInternalStageAttempt()
              }
            }
          }
          .build()
      val internalResponse: DuchyMeasurementLogEntry =
        measurementLogEntriesService.createDuchyMeasurementLogEntry(internalRequest)
      val measurementLogEntry = internalResponse.logEntry
      if (
        measurementLogEntry.externalMeasurementConsumerId != 0L &&
          measurementLogEntry.externalMeasurementId != 0L
      ) {
        Span.current()
          .setAttribute(
            ReportTraceAttributes.MEASUREMENT_NAME,
            MeasurementKey(
                externalIdToApiId(measurementLogEntry.externalMeasurementConsumerId),
                externalIdToApiId(measurementLogEntry.externalMeasurementId),
              )
              .toName(),
          )
      }
      Span.current().setAttribute(ReportTraceAttributes.OUTCOME, "accepted")
      internalResponse.toSystemComputationLogEntry(computationParticipantKey.computationId)
    }

  private fun logEntryTraceAttributes(parent: String): Attributes {
    val builder =
      Attributes.builder()
        .put(ReportTraceAttributes.LIFECYCLE_STAGE, "kingdom_computation_log_entry_acceptance")
        .put(ReportTraceAttributes.OUTCOME, "started")
    val key = ComputationParticipantKey.fromName(parent)
    if (key != null) {
      builder
        .put(ReportTraceAttributes.COMPUTATION_NAME, ComputationKey(key.computationId).toName())
        .put(ReportTraceAttributes.DUCHY_ID, key.duchyId)
    }
    return builder.build()
  }
}
