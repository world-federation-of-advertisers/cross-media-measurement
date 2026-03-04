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

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams

object DataAvailabilitySyncConfigParser {
  private val logger: Logger = Logger.getLogger("DataAvailabilitySyncConfigParser")

  private val typeRegistry: TypeRegistry =
    TypeRegistry.newBuilder()
      .add(DataAvailabilitySyncParams.getDescriptor())
      .add(DataAvailabilitySyncConfig.getDescriptor())
      .build()

  private val jsonParser: JsonFormat.Parser =
    JsonFormat.parser().usingTypeRegistry(typeRegistry).ignoringUnknownFields()

  private val configBlobKey: String by lazy {
    System.getenv("CONFIG_BLOB_KEY")
      ?: error("Environment variable CONFIG_BLOB_KEY must be set")
  }

  private val runtimeConfigs: DataAvailabilitySyncConfigs by lazy {
    runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        DataAvailabilitySyncConfigs.getDefaultInstance(),
      )
    }
  }

  /**
   * Parses the request body to produce a [DataAvailabilitySyncConfig].
   *
   * Supports three formats:
   * 1. **`google.protobuf.Any` wrapping [DataAvailabilitySyncParams]**: The `@type` field
   *    identifies the params proto. The `data_provider` field is used to look up the full
   *    per-EDP config from the runtime configs loaded at startup.
   * 2. **`google.protobuf.Any` wrapping [DataAvailabilitySyncConfig]**: The config is unpacked
   *    directly.
   * 3. **Legacy format**: The entire request body is parsed as [DataAvailabilitySyncConfig]
   *    directly (no `@type` field). This is the backwards-compatible path.
   *
   * @param requestBody JSON string from the HTTP request body
   * @return [DataAvailabilitySyncConfig] parsed from the appropriate format
   */
  fun parseDataAvailabilitySyncConfig(requestBody: String): DataAvailabilitySyncConfig {
    try {
      val any =
        Any.newBuilder().apply { jsonParser.merge(requestBody, this) }.build()

      return when {
        any.`is`(DataAvailabilitySyncParams::class.java) -> {
          val params = any.unpack(DataAvailabilitySyncParams::class.java)
          logger.info("Parsed request body as DataAvailabilitySyncParams (v1alpha) via @type")
          lookupConfig(params.dataProvider)
        }
        any.`is`(DataAvailabilitySyncConfig::class.java) -> {
          logger.info("Parsed request body as DataAvailabilitySyncConfig via @type")
          any.unpack(DataAvailabilitySyncConfig::class.java)
        }
        else -> throw InvalidProtocolBufferException("Unknown @type: ${any.typeUrl}")
      }
    } catch (e: InvalidProtocolBufferException) {
      logger.info("No @type found, parsing as DataAvailabilitySyncConfig (legacy)")
      return DataAvailabilitySyncConfig.newBuilder()
        .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
        .build()
    }
  }

  /**
   * Looks up a [DataAvailabilitySyncConfig] by data provider name from the runtime configs.
   *
   * @param dataProvider resource name of the data provider
   * @return the matching [DataAvailabilitySyncConfig]
   * @throws NoSuchElementException if no config is found for the given data provider
   */
  private fun lookupConfig(dataProvider: String): DataAvailabilitySyncConfig {
    return runtimeConfigs.configsList.first { it.dataProvider == dataProvider }
  }
}
