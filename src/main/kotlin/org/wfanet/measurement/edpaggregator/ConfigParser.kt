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

package org.wfanet.measurement.edpaggregator

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams

/**
 * Parses HTTP request bodies into typed config messages.
 *
 * Supports two formats:
 * 1. **`google.protobuf.Any` wrapping params**: The `@type` field identifies the params proto. The
 *    `data_provider` field is used to look up the full config from the provided configs list.
 * 2. **Legacy format**: The entire request body is parsed as the config proto directly (no `@type`
 *    field). This is the backwards-compatible path.
 */
object ConfigParser {
  private val logger: Logger = Logger.getLogger("ConfigParser")

  private val typeRegistry: TypeRegistry =
    TypeRegistry.newBuilder()
      .add(DataAvailabilitySyncParams.getDescriptor())
      .add(EventGroupSyncParams.getDescriptor())
      .build()

  /**
   * Builds a [DataAvailabilitySyncConfig] from the request body.
   *
   * @param requestBody JSON string from the HTTP request body
   * @param configs list of available configs to look up by data provider
   * @return [DataAvailabilitySyncConfig] parsed from the appropriate format
   * @throws NoSuchElementException if the data provider from params doesn't match any config
   * @throws IllegalStateException if the `@type` is not a supported type
   */
  fun buildDataAvailabilitySyncConfig(
    requestBody: String,
    configs: List<DataAvailabilitySyncConfig>,
  ): DataAvailabilitySyncConfig {
    val any = tryParseAsAny(requestBody)
      ?: return parseLegacy(requestBody, DataAvailabilitySyncConfig.getDefaultInstance())

    return when {
      any.`is`(DataAvailabilitySyncParams::class.java) -> {
        val params = any.unpack(DataAvailabilitySyncParams::class.java)
        logger.info("Parsed request body as DataAvailabilitySyncParams (v1alpha) via @type")
        configs.single { it.dataProvider == params.dataProvider }
      }
      else -> error("Unsupported @type: ${any.typeUrl}")
    }
  }

  /**
   * Builds an [EventGroupSyncConfig] from the request body.
   *
   * @param requestBody JSON string from the HTTP request body
   * @param configs list of available configs to look up by data provider
   * @return [EventGroupSyncConfig] parsed from the appropriate format
   * @throws NoSuchElementException if the data provider from params doesn't match any config
   * @throws IllegalStateException if the `@type` is not a supported type
   */
  fun buildEventGroupSyncConfig(
    requestBody: String,
    configs: List<EventGroupSyncConfig>,
  ): EventGroupSyncConfig {
    val any = tryParseAsAny(requestBody)
      ?: return parseLegacy(requestBody, EventGroupSyncConfig.getDefaultInstance())

    return when {
      any.`is`(EventGroupSyncParams::class.java) -> {
        val params = any.unpack(EventGroupSyncParams::class.java)
        logger.info("Parsed request body as EventGroupSyncParams (v1alpha) via @type")
        configs.single { it.dataProvider == params.dataProvider }
      }
      else -> error("Unsupported @type: ${any.typeUrl}")
    }
  }

  private fun tryParseAsAny(requestBody: String): Any? {
    return try {
      val any =
        Any.newBuilder()
          .apply {
            JsonFormat.parser()
              .ignoringUnknownFields()
              .usingTypeRegistry(typeRegistry)
              .merge(requestBody, this)
          }
          .build()
      if (any.typeUrl.isEmpty()) null else any
    } catch (e: InvalidProtocolBufferException) {
      null
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun <T : Message> parseLegacy(requestBody: String, defaultInstance: T): T {
    logger.info(
      "No @type found, parsing as ${defaultInstance.descriptorForType.fullName} (legacy)"
    )
    return defaultInstance
      .newBuilderForType()
      .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
      .build() as T
  }
}
