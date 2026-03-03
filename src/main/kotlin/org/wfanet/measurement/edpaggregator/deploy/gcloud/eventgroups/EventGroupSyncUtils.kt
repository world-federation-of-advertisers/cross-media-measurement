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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups

import com.google.gson.JsonParser
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams

object EventGroupSyncUtils {
  private val logger: Logger = Logger.getLogger("EventGroupSyncUtils")

  /**
   * Parses the request body using a `type_url` discriminator to determine the proto format.
   *
   * If the JSON contains a `type_url` (or `typeUrl`) field, the `value` field is parsed as the
   * proto type indicated by the URL suffix:
   * - `EventGroupSyncParams` -> v1alpha params, converted to config
   * - `EventGroupSyncConfig` -> config directly
   *
   * If no `type_url` is present, the entire request body is parsed as [EventGroupSyncConfig] for
   * backwards compatibility.
   *
   * @param requestBody JSON string from the HTTP request body
   * @return [EventGroupSyncConfig] parsed from the appropriate format
   */
  internal fun parseEventGroupSyncConfig(requestBody: String): EventGroupSyncConfig {
    val jsonObject = JsonParser.parseString(requestBody).asJsonObject
    val typeUrl = jsonObject.get("type_url")?.asString ?: jsonObject.get("typeUrl")?.asString

    if (typeUrl != null) {
      val valueElement =
        jsonObject.get("value")
          ?: throw InvalidProtocolBufferException("Missing 'value' field")
      val valueJson = valueElement.toString()

      return when {
        typeUrl.endsWith("EventGroupSyncParams") -> {
          logger.info("Parsed request body as EventGroupSyncParams (v1alpha) via type_url")
          val params =
            EventGroupSyncParams.newBuilder()
              .apply { JsonFormat.parser().ignoringUnknownFields().merge(valueJson, this) }
              .build()
          convertToConfig(params)
        }
        typeUrl.endsWith("EventGroupSyncConfig") -> {
          logger.info("Parsed request body as EventGroupSyncConfig via type_url")
          EventGroupSyncConfig.newBuilder()
            .apply { JsonFormat.parser().ignoringUnknownFields().merge(valueJson, this) }
            .build()
        }
        else -> throw InvalidProtocolBufferException("Unknown type_url: $typeUrl")
      }
    }

    logger.info("No type_url found, parsing as EventGroupSyncConfig (legacy)")
    return EventGroupSyncConfig.newBuilder()
      .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
      .build()
  }

  /**
   * Converts an [EventGroupSyncParams] to an [EventGroupSyncConfig].
   *
   * @param params v1alpha params to convert
   * @return [EventGroupSyncConfig] with equivalent field values
   */
  private fun convertToConfig(params: EventGroupSyncParams): EventGroupSyncConfig {
    return EventGroupSyncConfig.newBuilder()
      .apply {
        dataProvider = params.dataProvider
        eventGroupsBlobUri = params.eventGroupsBlobUri
        eventGroupMapBlobUri = params.eventGroupMapBlobUri
        cmmsConnection =
          TransportLayerSecurityParams.newBuilder()
            .apply {
              certFilePath = params.cmmsConnection.certFilePath
              privateKeyFilePath = params.cmmsConnection.privateKeyFilePath
              certCollectionFilePath = params.cmmsConnection.certCollectionFilePath
            }
            .build()
        eventGroupStorage =
          StorageParams.newBuilder()
            .apply {
              gcs =
                StorageParams.GcsStorage.newBuilder()
                  .apply {
                    projectId = params.eventGroupStorage.gcsProjectId
                    bucketName = params.eventGroupStorage.bucketName
                  }
                  .build()
            }
            .build()
        eventGroupMapStorage =
          StorageParams.newBuilder()
            .apply {
              gcs =
                StorageParams.GcsStorage.newBuilder()
                  .apply {
                    projectId = params.eventGroupMapStorage.gcsProjectId
                    bucketName = params.eventGroupMapStorage.bucketName
                  }
                  .build()
            }
            .build()
      }
      .build()
  }
}
