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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.eventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.eventGroupSyncParams

@RunWith(JUnit4::class)
class ConfigLoaderTest {

  @Test
  fun `buildDataAvailabilitySyncConfig parses legacy format`() {
    val config = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }
    val json = JsonFormat.printer().print(config)

    val result = ConfigLoader.buildDataAvailabilitySyncConfig(json, listOf(config))

    assertThat(result).isEqualTo(config)
  }

  @Test
  fun `buildDataAvailabilitySyncConfig returns matching config for Any-wrapped params`() {
    val config1 = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }
    val config2 = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp2" }
    val params = dataAvailabilitySyncParams { dataProvider = "dataProviders/edp2" }
    val anyJson = packToJson(ProtoAny.pack(params), DataAvailabilitySyncParams.getDescriptor())

    val result = ConfigLoader.buildDataAvailabilitySyncConfig(anyJson, listOf(config1, config2))

    assertThat(result).isEqualTo(config2)
  }

  @Test
  fun `buildDataAvailabilitySyncConfig throws for Any-wrapped params with non-matching data provider`() {
    val config = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }
    val params = dataAvailabilitySyncParams { dataProvider = "dataProviders/nonexistent" }
    val anyJson = packToJson(ProtoAny.pack(params), DataAvailabilitySyncParams.getDescriptor())

    assertFailsWith<NoSuchElementException> {
      ConfigLoader.buildDataAvailabilitySyncConfig(anyJson, listOf(config))
    }
  }

  @Test
  fun `buildDataAvailabilitySyncConfig throws for Any-wrapped params with unsupported type`() {
    val config = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }
    val params = eventGroupSyncParams { dataProvider = "dataProviders/edp1" }
    val anyJson = packToJson(ProtoAny.pack(params), EventGroupSyncParams.getDescriptor())

    assertFailsWith<IllegalStateException> {
      ConfigLoader.buildDataAvailabilitySyncConfig(anyJson, listOf(config))
    }
  }

  @Test
  fun `buildDataAvailabilitySyncConfig throws for malformed JSON`() {
    val config = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }

    assertFailsWith<InvalidProtocolBufferException> {
      ConfigLoader.buildDataAvailabilitySyncConfig("not valid json {{", listOf(config))
    }
  }

  @Test
  fun `buildDataAvailabilitySyncConfig throws for empty JSON object`() {
    val config = dataAvailabilitySyncConfig { dataProvider = "dataProviders/edp1" }

    assertFailsWith<IllegalStateException> {
      ConfigLoader.buildDataAvailabilitySyncConfig("{}", listOf(config))
    }
  }

  @Test
  fun `buildEventGroupSyncConfig parses legacy format`() {
    val config = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }
    val json = JsonFormat.printer().print(config)

    val result = ConfigLoader.buildEventGroupSyncConfig(json, listOf(config))

    assertThat(result).isEqualTo(config)
  }

  @Test
  fun `buildEventGroupSyncConfig returns matching config for Any-wrapped params`() {
    val config1 = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }
    val config2 = eventGroupSyncConfig { dataProvider = "dataProviders/edp2" }
    val params = eventGroupSyncParams { dataProvider = "dataProviders/edp2" }
    val anyJson = packToJson(ProtoAny.pack(params), EventGroupSyncParams.getDescriptor())

    val result = ConfigLoader.buildEventGroupSyncConfig(anyJson, listOf(config1, config2))

    assertThat(result).isEqualTo(config2)
  }

  @Test
  fun `buildEventGroupSyncConfig throws for Any-wrapped params with non-matching data provider`() {
    val config = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }
    val params = eventGroupSyncParams { dataProvider = "dataProviders/nonexistent" }
    val anyJson = packToJson(ProtoAny.pack(params), EventGroupSyncParams.getDescriptor())

    assertFailsWith<NoSuchElementException> {
      ConfigLoader.buildEventGroupSyncConfig(anyJson, listOf(config))
    }
  }

  @Test
  fun `buildEventGroupSyncConfig throws for Any-wrapped params with unsupported type`() {
    val config = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }
    val params = dataAvailabilitySyncParams { dataProvider = "dataProviders/edp1" }
    val anyJson = packToJson(ProtoAny.pack(params), DataAvailabilitySyncParams.getDescriptor())

    assertFailsWith<IllegalStateException> {
      ConfigLoader.buildEventGroupSyncConfig(anyJson, listOf(config))
    }
  }

  @Test
  fun `buildEventGroupSyncConfig throws for malformed JSON`() {
    val config = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }

    assertFailsWith<InvalidProtocolBufferException> {
      ConfigLoader.buildEventGroupSyncConfig("not valid json {{", listOf(config))
    }
  }

  @Test
  fun `buildEventGroupSyncConfig throws for empty JSON object`() {
    val config = eventGroupSyncConfig { dataProvider = "dataProviders/edp1" }

    assertFailsWith<IllegalStateException> {
      ConfigLoader.buildEventGroupSyncConfig("{}", listOf(config))
    }
  }

  companion object {
    private fun packToJson(
      protoAny: ProtoAny,
      vararg descriptors: com.google.protobuf.Descriptors.Descriptor,
    ): String {
      val typeRegistry =
        TypeRegistry.newBuilder()
          .apply {
            for (descriptor in descriptors) {
              add(descriptor)
            }
          }
          .build()
      return JsonFormat.printer().usingTypeRegistry(typeRegistry).print(protoAny)
    }
  }
}
