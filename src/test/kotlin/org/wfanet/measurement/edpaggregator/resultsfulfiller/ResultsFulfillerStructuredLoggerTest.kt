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
import com.google.cloud.logging.Payload
import com.google.common.truth.Truth.assertThat
import java.time.Instant
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

@RunWith(JUnit4::class)
class ResultsFulfillerStructuredLoggerTest {

  @Test
  fun `logRequisitionAudit emits expected JSON schema`() {
    val fakeLogging: Logging = mock()

    val groupedRequisitions =
      GroupedRequisitions.newBuilder()
        .setGroupId("group-123")
        .setModelLine("modelLines/abc")
        .build()

    val params =
      ResultsFulfillerParams.newBuilder()
        .setDataProvider("dataProviders/123")
        .build()

    val requisition =
      requisition {
        name = "dataProviders/123/requisitions/req-1"
        state = org.wfanet.measurement.api.v2alpha.Requisition.State.UNFULFILLED
      }

    val requisitionSpec = requisitionSpec {}

    val measurementResult: Measurement.Result = result { reach = reach { value = 42L } }

    val before = Instant.now()

    ResultsFulfillerStructuredLogger.logRequisitionAudit(
      groupId = groupedRequisitions.groupId,
      dataProvider = params.dataProvider,
      groupedRequisitions = groupedRequisitions,
      resultsFulfillerParams = params,
      requisitionsBlobUri = "gs://operator-bucket/requisitions/group-123",
      inputEventBlobUris =
        listOf(
          "gs://operator-bucket/impressions/blob-1",
          "gs://operator-bucket/impressions/blob-2",
        ),
      requisition = requisition,
      requisitionSpec = requisitionSpec,
      measurementResult = measurementResult,
      loggingOverride = fakeLogging,
      clockOverride = { Instant.now() },
    )

    verify(fakeLogging).write(argThat { entries ->
      assertThat(entries).hasSize(1)
      val entry = entries.first()
      val payload: Payload<*> = entry.getPayload()
      assertThat(payload).isInstanceOf(Payload.JsonPayload::class.java)
      val json = (payload as Payload.JsonPayload).dataAsMap

      assertThat(json["group_id"]).isEqualTo("group-123")
      assertThat(json["data_provider"]).isEqualTo("dataProviders/123")

      val ts = Instant.parse(json["timestamp"] as String)
      assertThat(ts).isAtLeast(before)

      @Suppress("UNCHECKED_CAST")
      val grouped = json["grouped_requisitions"] as Map<String, Any>
      assertThat(grouped["blob_uri"]).isEqualTo("gs://operator-bucket/requisitions/group-123")
      assertThat(grouped["data"]).isInstanceOf(String::class.java)
      @Suppress("UNCHECKED_CAST")
      val specMap = grouped[requisition.name] as Map<String, Any>
      assertThat(specMap).isNotNull()

      @Suppress("UNCHECKED_CAST")
      val dataSources = json["data_sources"] as Map<String, Any>
      @Suppress("UNCHECKED_CAST")
      val blobPaths = dataSources["blob_paths"] as Map<String, Any>
      @Suppress("UNCHECKED_CAST")
      val inputFiles = blobPaths["input_event_files"] as List<*>
      assertThat(inputFiles)
        .containsExactly(
          "gs://operator-bucket/impressions/blob-1",
          "gs://operator-bucket/impressions/blob-2",
        )

      @Suppress("UNCHECKED_CAST")
      val directResults = json["direct_results"] as Map<String, Any>
      assertThat(directResults.keys).containsExactly(requisition.name)
      @Suppress("UNCHECKED_CAST")
      val resultMap = directResults[requisition.name] as Map<String, Any>
      assertThat(resultMap).isNotNull()

      true
    })
  }
}
