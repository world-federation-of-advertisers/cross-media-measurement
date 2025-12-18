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
import com.google.protobuf.Any
import com.google.type.interval
import java.time.Instant
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

@RunWith(JUnit4::class)
class ResultsFulfillerStructuredLoggerTest {

  @Test
  fun `logRequisitionAudit emits expected JSON schema`() {
    val fakeLogging: Logging = mock()

    val eventGroupDetails =
      GroupedRequisitionsKt.eventGroupDetails {
        eventGroupReferenceId = "event-group-ref-1"
        collectionIntervals.add(
          interval {
            startTime = com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).build()
            endTime = com.google.protobuf.Timestamp.newBuilder().setSeconds(2000).build()
          }
        )
      }

    val groupedRequisitions = groupedRequisitions {
      groupId = "group-123"
      modelLine = "modelLines/abc"
      eventGroupMap.add(
        GroupedRequisitionsKt.eventGroupMapEntry {
          eventGroup = "dataProviders/123/eventGroups/eg-1"
          details = eventGroupDetails
        }
      )
    }

    val params = ResultsFulfillerParams.newBuilder().setDataProvider("dataProviders/123").build()

    val measurementSpecProto = measurementSpec { measurementPublicKey = Any.getDefaultInstance() }

    val requisition = requisition {
      name = "dataProviders/123/requisitions/req-1"
      state = org.wfanet.measurement.api.v2alpha.Requisition.State.UNFULFILLED
      measurementSpec = signedMessage { message = measurementSpecProto.pack() }
    }

    val requisitionSpecProto = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups += eventGroupEntry {
            key = "dataProviders/123/eventGroups/eg-1"
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).build()
                  endTime = com.google.protobuf.Timestamp.newBuilder().setSeconds(2000).build()
                }
                filter = eventFilter { expression = "person.age >= 18" }
              }
          }
        }
    }

    val measurementResult: Measurement.Result = result { reach = reach { value = 42L } }

    val testBlobUris = listOf("gs://bucket/blob1.rio", "gs://bucket/blob2.rio")

    val before = Instant.now()

    ResultsFulfillerStructuredLogger.logRequisitionAudit(
      groupId = groupedRequisitions.groupId,
      dataProvider = params.dataProvider,
      groupedRequisitions = groupedRequisitions,
      resultsFulfillerParams = params,
      requisitionsBlobUri = "gs://operator-bucket/requisitions/group-123",
      requisition = requisition,
      requisitionSpec = requisitionSpecProto,
      measurementResult = measurementResult,
      blobUris = testBlobUris,
      loggingOverride = fakeLogging,
      clockOverride = { Instant.now() },
      throwOnFailure = true,
    )

    val entriesCaptor = argumentCaptor<List<LogEntry>>()
    verify(fakeLogging).write(entriesCaptor.capture())

    assertThat(entriesCaptor.firstValue).hasSize(1)
    val entry = entriesCaptor.firstValue.first()
    val payload: Payload<*> = entry.getPayload()
    assertThat(payload).isInstanceOf(Payload.JsonPayload::class.java)
    val json = (payload as Payload.JsonPayload).dataAsMap

    // Verify the new schema
    assertThat(json["groupedRequisitionId"]).isEqualTo("group-123")
    assertThat(json["dataProvider"]).isEqualTo("dataProviders/123")

    // Verify timestamp
    val timestampString = json["timestamp"] as String
    val recordedInstant = Instant.parse(timestampString)

    // Verify requisition is present
    @Suppress("UNCHECKED_CAST") val requisitionJson = json["requisition"] as Map<String, Any>
    assertThat(requisitionJson["name"]).isEqualTo("dataProviders/123/requisitions/req-1")

    // Verify requisition_spec is present
    @Suppress("UNCHECKED_CAST")
    val requisitionSpecJson = json["requisitionSpec"] as Map<String, Any>
    assertThat(requisitionSpecJson).isNotNull()

    // Verify results_fulfiller_params is present
    @Suppress("UNCHECKED_CAST") val paramsJson = json["resultsFulfillerParams"] as Map<String, Any>
    assertThat(paramsJson["dataProvider"]).isEqualTo("dataProviders/123")

    // Verify cel_expression
    assertThat(json["celExpression"]).isEqualTo("person.age >= 18")

    // Verify collection_interval is present
    @Suppress("UNCHECKED_CAST")
    val collectionInterval = json["collectionInterval"] as Map<String, Any>
    assertThat(collectionInterval).isNotNull()

    // Verify event_group_reference_ids
    @Suppress("UNCHECKED_CAST") val eventGroupRefIds = json["eventGroupReferenceIds"] as List<*>
    assertThat(eventGroupRefIds).containsExactly("event-group-ref-1")

    // Verify measurement_spec is present
    @Suppress("UNCHECKED_CAST")
    val measurementSpecJson = json["measurementSpec"] as Map<String, Any>
    assertThat(measurementSpecJson).isNotNull()

    // Verify blob_uris
    @Suppress("UNCHECKED_CAST") val blobUrisJson = json["blobUris"] as List<*>
    assertThat(blobUrisJson).containsExactlyElementsIn(testBlobUris)

    // Verify measurement_result
    @Suppress("UNCHECKED_CAST")
    val measurementResultJson = json["measurementResult"] as Map<String, Any>
    @Suppress("UNCHECKED_CAST") val reachJson = measurementResultJson["reach"] as Map<String, Any>
    val reachValue = reachJson["value"].toString().toDouble()
    assertThat(reachValue.toLong()).isEqualTo(42L)

    // Verify timestamp is within reasonable bounds
    val after = Instant.now()
    assertThat(recordedInstant.epochSecond).isAtLeast(before.epochSecond)
    assertThat(recordedInstant.epochSecond).isAtMost(after.epochSecond)
  }
}
