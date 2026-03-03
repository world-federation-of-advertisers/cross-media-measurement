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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.gson.JsonParser
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams

object DataAvailabilitySyncUtils {
  private val logger: Logger = Logger.getLogger("DataAvailabilitySyncUtils")

  /**
   * Parses the request body using a `type_url` discriminator to determine the proto format.
   *
   * If the JSON contains a `type_url` (or `typeUrl`) field, the `value` field is parsed as the
   * proto type indicated by the URL suffix:
   * - `DataAvailabilitySyncParams` -> v1alpha params, converted to config
   * - `DataAvailabilitySyncConfig` -> config directly
   *
   * If no `type_url` is present, the entire request body is parsed as
   * [DataAvailabilitySyncConfig] for backwards compatibility.
   *
   * @param requestBody JSON string from the HTTP request body
   * @return [DataAvailabilitySyncConfig] parsed from the appropriate format
   */
  fun parseDataAvailabilitySyncConfig(requestBody: String): DataAvailabilitySyncConfig {
    val jsonObject = JsonParser.parseString(requestBody).asJsonObject
    val typeUrl = jsonObject.get("type_url")?.asString ?: jsonObject.get("typeUrl")?.asString

    if (typeUrl != null) {
      val valueElement =
        jsonObject.get("value")
          ?: throw InvalidProtocolBufferException("Missing 'value' field")
      val valueJson = valueElement.toString()

      return when {
        typeUrl.endsWith("DataAvailabilitySyncParams") -> {
          logger.info("Parsed request body as DataAvailabilitySyncParams (v1alpha) via type_url")
          val params =
            DataAvailabilitySyncParams.newBuilder()
              .apply { JsonFormat.parser().ignoringUnknownFields().merge(valueJson, this) }
              .build()
          convertToConfig(params)
        }
        typeUrl.endsWith("DataAvailabilitySyncConfig") -> {
          logger.info("Parsed request body as DataAvailabilitySyncConfig via type_url")
          DataAvailabilitySyncConfig.newBuilder()
            .apply { JsonFormat.parser().ignoringUnknownFields().merge(valueJson, this) }
            .build()
        }
        else -> throw InvalidProtocolBufferException("Unknown type_url: $typeUrl")
      }
    }

    logger.info("No type_url found, parsing as DataAvailabilitySyncConfig (legacy)")
    return DataAvailabilitySyncConfig.newBuilder()
      .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
      .build()
  }

  /**
   * Converts a [DataAvailabilitySyncParams] to a [DataAvailabilitySyncConfig].
   *
   * @param params v1alpha params to convert
   * @return [DataAvailabilitySyncConfig] with equivalent field values
   */
  private fun convertToConfig(params: DataAvailabilitySyncParams): DataAvailabilitySyncConfig {
    return DataAvailabilitySyncConfig.newBuilder()
      .apply {
        dataProvider = params.dataProvider
        dataAvailabilityStorage =
          StorageParams.newBuilder()
            .apply {
              gcs =
                StorageParams.GcsStorage.newBuilder()
                  .apply {
                    projectId = params.dataAvailabilityStorage.gcsProjectId
                    bucketName = params.dataAvailabilityStorage.bucketName
                  }
                  .build()
            }
            .build()
        cmmsConnection =
          TransportLayerSecurityParams.newBuilder()
            .apply {
              certFilePath = params.cmmsConnection.certFilePath
              privateKeyFilePath = params.cmmsConnection.privateKeyFilePath
              certCollectionFilePath = params.cmmsConnection.certCollectionFilePath
            }
            .build()
        impressionMetadataStorageConnection =
          TransportLayerSecurityParams.newBuilder()
            .apply {
              certFilePath = params.impressionMetadataStorageConnection.certFilePath
              privateKeyFilePath =
                params.impressionMetadataStorageConnection.privateKeyFilePath
              certCollectionFilePath =
                params.impressionMetadataStorageConnection.certCollectionFilePath
            }
            .build()
        edpImpressionPath = params.edpImpressionPath
        params.modelLineMapMap.forEach { (key, value) ->
          putModelLineMap(
            key,
            DataAvailabilitySyncConfig.ModelLineList.newBuilder()
              .addAllModelLines(value.modelLinesList)
              .build(),
          )
        }
      }
      .build()
  }
}
