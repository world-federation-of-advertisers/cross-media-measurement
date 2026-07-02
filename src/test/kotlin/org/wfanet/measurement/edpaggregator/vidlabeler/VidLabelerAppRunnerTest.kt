/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt

@RunWith(JUnit4::class)
class VidLabelerAppRunnerTest {

  companion object {
    init {
      // The runner extends BaseTeeAppRunner, which autoconfigures OpenTelemetry; disable the
      // exporters so constructing it in-process does not reach out to Cloud Monitoring.
      System.setProperty("otel.metrics.exporter", "none")
      System.setProperty("otel.traces.exporter", "none")
      System.setProperty("otel.logs.exporter", "none")
    }
  }

  @Test
  fun `resolveEventDescriptor throws when event_template_descriptor_blob_uri is empty`() {
    // Type set so the descriptor-blob-uri guard is the one that fires.
    val config =
      VidLabelerParamsKt.modelLineConfig {
        eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        runBlocking { VidLabelerAppRunner().resolveEventDescriptor(config) }
      }
    assertThat(exception)
      .hasMessageThat()
      .contains("event_template_descriptor_blob_uri must be set")
  }

  @Test
  fun `resolveEventDescriptor throws when event_template_type is empty`() {
    // Blob URI set so the descriptor-type guard is the one that fires.
    val config =
      VidLabelerParamsKt.modelLineConfig {
        eventTemplateDescriptorBlobUri = "gs://bucket/event-template-descriptor-set.binpb"
      }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        runBlocking { VidLabelerAppRunner().resolveEventDescriptor(config) }
      }
    assertThat(exception).hasMessageThat().contains("event_template_type must be set")
  }
}
