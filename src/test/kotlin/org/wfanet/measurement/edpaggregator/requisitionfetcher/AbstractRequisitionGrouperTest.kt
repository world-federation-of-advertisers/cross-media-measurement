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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.StringValue
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData

@RunWith(JUnit4::class)
abstract class AbstractRequisitionGrouperTest {
  abstract val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase

  abstract val eventGroupsServiceMock: EventGroupsCoroutineImplBase

  @get:Rule abstract val grpcTestServerRule: GrpcTestServerRule

  protected abstract val requisitionGrouper: RequisitionGrouper

  @Test
  fun `does not skip valid requisition`() {

    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenReturn(eventGroup { eventGroupReferenceId = "some-event-group-reference-id" })
    }
    val groupedRequisitions = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION))
    }
    assertTrue(groupedRequisitions.isNotEmpty())
  }

  @Test
  fun `skips Requisition when Measurement Spec cannot be parsed`() {

    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenReturn(eventGroup { eventGroupReferenceId = "some-event-group-reference-id" })
    }
    val requisition =
      TestRequisitionData.REQUISITION.copy {
        measurementSpec = signedMessage {
          message = Any.pack(StringValue.newBuilder().setValue("some-invalid-spec").build())
        }
      }
    val groupedRequisitions = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(requisition))
    }
    assertThat(groupedRequisitions).hasSize(0)
  }

  @Test
  fun `skips Requisition when Requisition Spec cannot be parsed`() {

    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenReturn(eventGroup { eventGroupReferenceId = "some-event-group-reference-id" })
    }
    val requisition =
      TestRequisitionData.REQUISITION.copy {
        encryptedRequisitionSpec = encryptedMessage {
          ciphertext = "some-invalid-spec".toByteStringUtf8()
          typeUrl = ProtoReflection.getTypeUrl(RequisitionSpec.getDescriptor())
        }
      }
    val groupedRequisitions = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(requisition))
    }
    assertThat(groupedRequisitions).hasSize(0)
  }
}
