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

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfig
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams

object EventGroupSyncConfigParser {
  private val logger: Logger = Logger.getLogger("EventGroupSyncConfigParser")

  private val typeRegistry: TypeRegistry =
    TypeRegistry.newBuilder()
      .add(EventGroupSyncParams.getDescriptor())
      .add(EventGroupSyncConfig.getDescriptor())
      .build()

  private val jsonParser: JsonFormat.Parser =
    JsonFormat.parser().usingTypeRegistry(typeRegistry).ignoringUnknownFields()

  private val configBlobKey: String by lazy {
    System.getenv("CONFIG_BLOB_KEY")
      ?: error("Environment variable CONFIG_BLOB_KEY must be set")
  }

  private val runtimeConfigs: EventGroupSyncConfigs by lazy {
    runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        EventGroupSyncConfigs.getDefaultInstance(),
      )
    }
  }

  /**
   * Parses the request body to produce an [EventGroupSyncConfig].
   *
   * Supports three formats:
   * 1. **`google.protobuf.Any` wrapping [EventGroupSyncParams]**: The `@type` field identifies
   *    the params proto. The `data_provider` field is used to look up the full per-EDP config
   *    from the runtime configs loaded at startup.
   * 2. **`google.protobuf.Any` wrapping [EventGroupSyncConfig]**: The config is unpacked
   *    directly.
   * 3. **Legacy format**: The entire request body is parsed as [EventGroupSyncConfig] directly
   *    (no `@type` field). This is the backwards-compatible path.
   *
   * @param requestBody JSON string from the HTTP request body
   * @return [EventGroupSyncConfig] parsed from the appropriate format
   */
  internal fun parseEventGroupSyncConfig(requestBody: String): EventGroupSyncConfig {
    try {
      val any =
        Any.newBuilder().apply { jsonParser.merge(requestBody, this) }.build()

      return when {
        any.`is`(EventGroupSyncParams::class.java) -> {
          val params = any.unpack(EventGroupSyncParams::class.java)
          logger.info("Parsed request body as EventGroupSyncParams (v1alpha) via @type")
          lookupConfig(params.dataProvider)
        }
        any.`is`(EventGroupSyncConfig::class.java) -> {
          logger.info("Parsed request body as EventGroupSyncConfig via @type")
          any.unpack(EventGroupSyncConfig::class.java)
        }
        else -> throw InvalidProtocolBufferException("Unknown @type: ${any.typeUrl}")
      }
    } catch (e: InvalidProtocolBufferException) {
      logger.info("No @type found, parsing as EventGroupSyncConfig (legacy)")
      return EventGroupSyncConfig.newBuilder()
        .apply { JsonFormat.parser().ignoringUnknownFields().merge(requestBody, this) }
        .build()
    }
  }

  /**
   * Looks up an [EventGroupSyncConfig] by data provider name from the runtime configs.
   *
   * @param dataProvider resource name of the data provider
   * @return the matching [EventGroupSyncConfig]
   * @throws NoSuchElementException if no config is found for the given data provider
   */
  private fun lookupConfig(dataProvider: String): EventGroupSyncConfig {
    return runtimeConfigs.configsList.first { it.dataProvider == dataProvider }
  }
}
