/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.logging.Payload
import com.google.cloud.logging.Severity
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.JsonFormat.TypeRegistry
import java.time.Instant
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionAuditLog
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionAuditLog

/**
 * Structured JSON logger for Results Fulfiller audit events.
 *
 * Writes entries directly to Cloud Logging using JsonPayload. Each entry is a
 * RequisitionAuditLog protobuf message converted to JSON with the schema:
 * - grouped_requisition_id: string
 * - data_provider: string
 * - timestamp: ISO8601
 * - requisition: Requisition proto (as JSON)
 * - requisition_spec: RequisitionSpec proto (as JSON)
 * - results_fulfiller_params: ResultsFulfillerParams proto (as JSON)
 * - cel_expression: string
 * - collection_interval: Interval proto (as JSON)
 * - event_group_reference_ids: [string]
 * - measurement_spec: MeasurementSpec proto (as JSON)
 */
object ResultsFulfillerStructuredLogger {
  private val logger: Logger =
    Logger.getLogger(ResultsFulfillerStructuredLogger::class.java.name)

  private val logging: Logging by lazy { LoggingOptions.getDefaultInstance().service }
  private val typeRegistry: TypeRegistry by lazy {
    TypeRegistry.newBuilder()
      .add(EncryptionPublicKey.getDescriptor())
      .add(MeasurementSpec.getDescriptor())
      .add(RequisitionSpec.getDescriptor())
      .add(GroupedRequisitions.getDescriptor())
      .add(ResultsFulfillerParams.getDescriptor())
      .build()
  }

  private const val LOG_NAME = "results_fulfiller_audit"

  /**
   * Emits a structured audit log entry for a single requisition within a group.
   *
   * Logging failures are swallowed and reported at FINE level to avoid impacting the pipeline.
   */
  fun logRequisitionAudit(
    groupId: String,
    dataProvider: String,
    groupedRequisitions: GroupedRequisitions,
    resultsFulfillerParams: ResultsFulfillerParams,
    requisitionsBlobUri: String,
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementResult: Measurement.Result?,
    blobUris: List<String>,
    loggingOverride: Logging? = null,
    clockOverride: () -> Instant = { Instant.now() },
    throwOnFailure: Boolean = false,
  ) {
    try {
      val now = clockOverride()
      val timestamp = Timestamp.newBuilder()
        .setSeconds(now.epochSecond)
        .setNanos(now.nano)
        .build()

      // Extract event group reference IDs from the grouped requisitions
      val eventGroupReferenceIds = groupedRequisitions.eventGroupMapList
        .map { it.details.eventGroupReferenceId }
        .filter { it.isNotEmpty() }

      // Extract CEL expression and collection interval from the first event group in requisition spec
      val firstEventGroup = requisitionSpec.events.eventGroupsList.firstOrNull()
      val celExpression = firstEventGroup?.value?.filter?.expression ?: ""
      val collectionInterval = firstEventGroup?.value?.collectionInterval

      // Build the audit log envelope
      val auditLog = requisitionAuditLog {
        groupedRequisitionId = groupId
        this.dataProvider = dataProvider
        this.timestamp = timestamp
        this.requisition = requisition
        this.requisitionSpec = requisitionSpec
        this.resultsFulfillerParams = resultsFulfillerParams
        this.celExpression = celExpression
        if (collectionInterval != null) {
          this.collectionInterval = collectionInterval
        }
        this.eventGroupReferenceIds.addAll(eventGroupReferenceIds)
        this.measurementSpec = requisition.measurementSpec.message.unpack(MeasurementSpec::class.java)
        this.blobUris.addAll(blobUris)
        if (measurementResult != null) {
          this.measurementResult = measurementResult
        }
      }

      // Convert the proto to JSON string using standard protobuf JSON conversion
      val jsonString = JsonFormat.printer()
        .includingDefaultValueFields()
        .usingTypeRegistry(typeRegistry)
        .print(auditLog)

      // Parse the JSON string into a Map for Cloud Logging
      val payloadMap = parseJsonToMap(jsonString)

      val entry =
        LogEntry.newBuilder(Payload.JsonPayload.of(payloadMap))
          .setSeverity(Severity.INFO)
          .setLogName(LOG_NAME)
          .build()

      val client = loggingOverride ?: logging
      client.write(listOf(entry))
    } catch (t: Throwable) {
      // Do not fail the fulfiller if audit logging fails.
      logger.log(Level.FINE, "Failed to write structured results fulfiller audit log", t)
      if (throwOnFailure) {
        throw t
      }
    }
  }

  /** Parses a JSON string into a Map for Cloud Logging. */
  private fun parseJsonToMap(jsonString: String): Map<String, Any> {
    @Suppress("UNCHECKED_CAST")
    return com.google.gson.Gson().fromJson(jsonString, Map::class.java) as Map<String, Any>
  }
}
