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
   * Parses the request body as either a [DataAvailabilitySyncParams] (v1alpha) or a
   * [DataAvailabilitySyncConfig] (config).
   *
   * Tries the v1alpha format first. If parsing fails due to unknown fields, falls back to the
   * legacy config format.
   *
   * @param requestBody JSON string from the HTTP request body
   * @return [DataAvailabilitySyncConfig] parsed from either format
   */
  fun parseDataAvailabilitySyncConfig(requestBody: String): DataAvailabilitySyncConfig {
    return try {
      val params =
        DataAvailabilitySyncParams.newBuilder()
          .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
          .build()
      logger.info("Parsed request body as DataAvailabilitySyncParams (v1alpha)")
      convertToConfig(params)
    } catch (e: InvalidProtocolBufferException) {
      logger.info("Falling back to DataAvailabilitySyncConfig (config)")
      DataAvailabilitySyncConfig.newBuilder()
        .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
        .build()
    }
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
