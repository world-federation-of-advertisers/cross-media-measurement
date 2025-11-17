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
import com.google.protobuf.ByteString
import java.time.Instant
import java.util.Base64
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

/**
 * Structured JSON logger for Results Fulfiller audit events.
 *
 * Writes entries directly to Cloud Logging using JsonPayload with the schema:
 * - group_id: string (primary key)
 * - data_provider: string
 * - timestamp: ISO8601
 * - grouped_requisitions: {
 *     data: base64(GroupedRequisitions),
 *     blob_uri: string,
 *     proto: {
 *       model_line: string,
 *       event_group_map: [ ... ],
 *       requisitions: [ ... ],
 *       group_id: string
 *     },
 *     "<requisition name>": {
 *       // Structured decrypted requisition spec. See RequisitionSpec proto.
 *       ...
 *     }
 *   }
 * - params: {
 *     data_provider: string,
 *     storage_params: {
 *       labeled_impressions_blob_details_uri_prefix: string,
 *       gcs_project_id: string
 *     },
 *     noise_params: {
 *       noise_type: string
 *     },
 *     k_anonymity_params: {
 *       min_impressions: int,
 *       min_users: int,
 *       reach_max_frequency_per_user: int
 *     }
 *   }
 *   (TLS and consent params are intentionally omitted.)
 * - data_sources: {
 *     blob_paths: {
 *       input_event_files: [string]
 *     }
 *   }
 * - direct_results: {
 *     "<requisition id>": {
 *       // Structured Measurement.Result. See Measurement.Result proto.
 *       ...
 *     }
 *   }
 */
object ResultsFulfillerStructuredLogger {
  private val logger: Logger =
    Logger.getLogger(ResultsFulfillerStructuredLogger::class.java.name)

  private val logging: Logging by lazy { LoggingOptions.getDefaultInstance().service }

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
    inputEventBlobUris: List<String>,
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementResult: Measurement.Result?,
    loggingOverride: Logging? = null,
    clockOverride: () -> Instant = { Instant.now() },
  ) {
    try {
      val groupedRequisitionsData =
        Base64.getEncoder().encodeToString(groupedRequisitions.toByteArray())

      val groupedRequisitionsMap: MutableMap<String, Any> =
        mutableMapOf(
          "data" to groupedRequisitionsData,
          "blob_uri" to requisitionsBlobUri,
          "proto" to groupedRequisitions.toStructuredMap(),
        )
      // Attach the decrypted requisition spec under the requisition name key as structured data.
      groupedRequisitionsMap[requisition.name] = requisitionSpec.toStructuredMap()

      val directResultsMap: Map<String, Any> =
        if (measurementResult != null) {
          mapOf(requisition.name to measurementResult.toStructuredMap())
        } else {
          emptyMap()
        }

      val payloadMap: Map<String, Any> =
        mapOf(
          "group_id" to groupId,
          "data_provider" to dataProvider,
          "timestamp" to clockOverride().toString(),
          "grouped_requisitions" to groupedRequisitionsMap,
          "params" to resultsFulfillerParams.toStructuredMap(),
          "data_sources" to
            mapOf(
              "blob_paths" to
                mapOf(
                  "input_event_files" to inputEventBlobUris,
                ),
            ),
          "direct_results" to directResultsMap,
        )

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
    }
  }

  private fun ResultsFulfillerParams.toStructuredMap(): Map<String, Any> {
    val paramsMap: MutableMap<String, Any> = mutableMapOf()

    if (dataProvider.isNotEmpty()) {
      paramsMap["data_provider"] = dataProvider
    }

    if (hasStorageParams()) {
      val storageParamsMap: MutableMap<String, Any> = mutableMapOf()
      if (storageParams.labeledImpressionsBlobDetailsUriPrefix.isNotEmpty()) {
        storageParamsMap["labeled_impressions_blob_details_uri_prefix"] =
          storageParams.labeledImpressionsBlobDetailsUriPrefix
      }
      if (storageParams.gcsProjectId.isNotEmpty()) {
        storageParamsMap["gcs_project_id"] = storageParams.gcsProjectId
      }
      if (storageParamsMap.isNotEmpty()) {
        paramsMap["storage_params"] = storageParamsMap
      }
    }

    if (hasNoiseParams()) {
      val noiseParamsMap: MutableMap<String, Any> = mutableMapOf()
      if (noiseParams.noiseType != ResultsFulfillerParams.NoiseParams.NoiseType.UNSPECIFIED) {
        noiseParamsMap["noise_type"] = noiseParams.noiseType.name
      }
      if (noiseParamsMap.isNotEmpty()) {
        paramsMap["noise_params"] = noiseParamsMap
      }
    }

    if (hasKAnonymityParams()) {
      val kAnonymityParamsMap: MutableMap<String, Any> = mutableMapOf()

      if (kAnonymityParams.minImpressions != 0) {
        kAnonymityParamsMap["min_impressions"] = kAnonymityParams.minImpressions
      }
      if (kAnonymityParams.minUsers != 0) {
        kAnonymityParamsMap["min_users"] = kAnonymityParams.minUsers
      }
      if (kAnonymityParams.reachMaxFrequencyPerUser != 0) {
        kAnonymityParamsMap["reach_max_frequency_per_user"] =
          kAnonymityParams.reachMaxFrequencyPerUser
      }

      if (kAnonymityParamsMap.isNotEmpty()) {
        paramsMap["k_anonymity_params"] = kAnonymityParamsMap
      }
    }

    return paramsMap
  }

  private fun GroupedRequisitions.toStructuredMap(): Map<String, Any> {
    val groupedMap: MutableMap<String, Any> = mutableMapOf()

    if (modelLine.isNotEmpty()) {
      groupedMap["model_line"] = modelLine
    }

    if (eventGroupMapCount > 0) {
      groupedMap["event_group_map"] =
        eventGroupMapList.map { entry ->
          val entryMap: MutableMap<String, Any> = mutableMapOf()
          if (entry.eventGroup.isNotEmpty()) {
            entryMap["event_group"] = entry.eventGroup
          }
          if (entry.hasDetails()) {
            val details = entry.details
            val detailsMap: MutableMap<String, Any> = mutableMapOf()
            if (details.eventGroupReferenceId.isNotEmpty()) {
              detailsMap["event_group_reference_id"] = details.eventGroupReferenceId
            }
            if (details.collectionIntervalsCount > 0) {
              detailsMap["collection_intervals"] =
                details.collectionIntervalsList.map { interval ->
                  val intervalMap: MutableMap<String, Any> = mutableMapOf()
                  if (interval.hasStartTime()) {
                    intervalMap["start_time"] = interval.startTime.toString()
                  }
                  if (interval.hasEndTime()) {
                    intervalMap["end_time"] = interval.endTime.toString()
                  }
                  intervalMap
                }
            }
            if (detailsMap.isNotEmpty()) {
              entryMap["details"] = detailsMap
            }
          }
          entryMap
        }
    }

    if (requisitionsCount > 0) {
      groupedMap["requisitions"] =
        requisitionsList.map { entry ->
          val entryMap: MutableMap<String, Any> = mutableMapOf()
          if (entry.hasRequisition()) {
            val anyRequisition = entry.requisition
            if (anyRequisition.typeUrl.isNotEmpty()) {
              entryMap["type_url"] = anyRequisition.typeUrl
            }
            if (!anyRequisition.value.isEmpty) {
              entryMap["value"] =
                Base64.getEncoder().encodeToString(anyRequisition.value.toByteArray())
            }
          }
          entryMap
        }
    }

    if (groupId.isNotEmpty()) {
      groupedMap["group_id"] = groupId
    }

    return groupedMap
  }

  private fun RequisitionSpec.toStructuredMap(): Map<String, Any> {
    val specMap: MutableMap<String, Any> = mutableMapOf()

    if (hasEvents()) {
      val eventsMap: MutableMap<String, Any> = mutableMapOf()
      if (events.eventGroupsCount > 0) {
        eventsMap["event_groups"] =
          events.eventGroupsList.map { entry ->
            val entryMap: MutableMap<String, Any> = mutableMapOf()
            if (entry.key.isNotEmpty()) {
              entryMap["event_group"] = entry.key
            }
            if (entry.hasValue()) {
              val value = entry.value
              val valueMap: MutableMap<String, Any> = mutableMapOf()
              if (value.hasCollectionInterval()) {
                val interval = value.collectionInterval
                val intervalMap: MutableMap<String, Any> = mutableMapOf()
                if (interval.hasStartTime()) {
                  intervalMap["start_time"] = interval.startTime.toString()
                }
                if (interval.hasEndTime()) {
                  intervalMap["end_time"] = interval.endTime.toString()
                }
                if (intervalMap.isNotEmpty()) {
                  valueMap["collection_interval"] = intervalMap
                }
              }
              if (value.hasFilter()) {
                val filter = value.filter
                if (filter.expression.isNotEmpty()) {
                  valueMap["filter"] = mapOf("expression" to filter.expression)
                }
              }
              if (valueMap.isNotEmpty()) {
                entryMap["value"] = valueMap
              }
            }
            entryMap
          }
      }
      if (eventsMap.isNotEmpty()) {
        specMap["events"] = eventsMap
      }
    }

    if (hasMeasurementPublicKey()) {
      val any = measurementPublicKey
      val keyMap: MutableMap<String, Any> = mutableMapOf()
      if (any.typeUrl.isNotEmpty()) {
        keyMap["type_url"] = any.typeUrl
      }
      if (!any.value.isEmpty) {
        keyMap["value"] = Base64.getEncoder().encodeToString(any.value.toByteArray())
      }
      if (keyMap.isNotEmpty()) {
        specMap["measurement_public_key"] = keyMap
      }
    }

    if (nonce != 0L) {
      specMap["nonce"] = nonce
    }

    return specMap
  }

  private fun Measurement.Result.toStructuredMap(): Map<String, Any> {
    val resultMap: MutableMap<String, Any> = mutableMapOf()

    if (hasReach()) {
      resultMap["reach"] = mapOf("value" to reach.value)
    }
    if (hasFrequency()) {
      val frequencyMap: MutableMap<String, Any> = mutableMapOf()
      if (frequency.relativeFrequencyDistributionCount > 0) {
        frequencyMap["relative_frequency_distribution"] =
          frequency.relativeFrequencyDistributionMap.mapKeys { (k, _) -> k.toString() }
      }
      if (frequencyMap.isNotEmpty()) {
        resultMap["frequency"] = frequencyMap
      }
    }
    if (hasImpression()) {
      resultMap["impression"] = mapOf("value" to impression.value)
    }
    if (hasWatchDuration()) {
      val duration = watchDuration.value
      resultMap["watch_duration"] =
        mapOf(
          "seconds" to duration.seconds,
          "nanos" to duration.nanos,
        )
    }
    if (hasPopulation()) {
      resultMap["population"] = mapOf("value" to population.value)
    }

    return resultMap
  }
}
