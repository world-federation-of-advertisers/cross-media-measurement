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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupActivityRequest
import org.wfanet.measurement.api.v2alpha.batchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.deleteEventGroupActivityRequest
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupActivitiesResponse as InternalBatchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.internal.kingdom.batchDeleteEventGroupActivitiesRequest as internalBatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesRequest as internalBatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesResponse as internalBatchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.deleteEventGroupActivityRequest as internalDeleteEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.eventGroupActivity as internalEventGroupActivity
import org.wfanet.measurement.internal.kingdom.updateEventGroupActivityRequest as internalUpdateEventGroupActivityRequest

private const val DATA_PROVIDER_EXTERNAL_ID = 123L
private val DATA_PROVIDER_NAME = makeDataProvider(DATA_PROVIDER_EXTERNAL_ID)
private val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
private val EVENT_GROUP_EXTERNAL_ID =
  apiIdToExternalId(EventGroupKey.fromName(EVENT_GROUP_NAME)!!.eventGroupId)
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class EventGroupActivitiesServiceTest {

  private val internalServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { batchUpdateEventGroupActivities(any()) }
      .thenReturn(InternalBatchUpdateEventGroupActivitiesResponse.getDefaultInstance())
    onBlocking { deleteEventGroupActivity(any()) }.thenReturn(Empty.getDefaultInstance())
    onBlocking { batchDeleteEventGroupActivities(any()) }.thenReturn(Empty.getDefaultInstance())
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: EventGroupActivitiesService

  @Before
  fun initService() {
    service =
      EventGroupActivitiesService(EventGroupActivitiesCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `batchUpdateEventGroupActivities returns created EventGroupActivities`() {
    val activityDate = date {
      year = 2023
      month = 10
      day = 10
    }
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
          date = activityDate
        }
        allowMissing = true
      }
    }

    val internalResponse = internalBatchUpdateEventGroupActivitiesResponse {
      eventGroupActivities += createInternalEventGroupActivity(activityDate)
    }

    internalServiceMock.stub {
      onBlocking { batchUpdateEventGroupActivities(any()) }.thenReturn(internalResponse)
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchUpdateEventGroupActivities(request) }
      }

    verifyProtoArgument(
        internalServiceMock,
        EventGroupActivitiesCoroutineImplBase::batchUpdateEventGroupActivities,
      )
      .isEqualTo(
        internalBatchUpdateEventGroupActivitiesRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          requests += internalUpdateEventGroupActivityRequest {
            eventGroupActivity = createInternalEventGroupActivity(activityDate)
            allowMissing = true
          }
        }
      )

    assertThat(response)
      .isEqualTo(
        batchUpdateEventGroupActivitiesResponse {
          eventGroupActivities += eventGroupActivity {
            name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
            date = activityDate
          }
        }
      )
  }

  @Test
  fun `batchUpdateEventGroupActivities returns updated existing EventGroupActivities`() {
    val activityDate = date {
      year = 2023
      month = 2
      day = 2
    }
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-02-02"
          date = activityDate
        }
        allowMissing = false
      }
    }

    val internalResponse = internalBatchUpdateEventGroupActivitiesResponse {
      eventGroupActivities += createInternalEventGroupActivity(activityDate)
    }

    internalServiceMock.stub {
      onBlocking { batchUpdateEventGroupActivities(any()) }.thenReturn(internalResponse)
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchUpdateEventGroupActivities(request) }
      }

    verifyProtoArgument(
        internalServiceMock,
        EventGroupActivitiesCoroutineImplBase::batchUpdateEventGroupActivities,
      )
      .isEqualTo(
        internalBatchUpdateEventGroupActivitiesRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          requests += internalUpdateEventGroupActivityRequest {
            eventGroupActivity = createInternalEventGroupActivity(activityDate)
            allowMissing = false
          }
        }
      )

    assertThat(response)
      .isEqualTo(
        batchUpdateEventGroupActivitiesResponse {
          eventGroupActivities += eventGroupActivity {
            name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-02-02"
            date = activityDate
          }
        }
      )
  }

  @Test
  fun `batchUpdateEventGroupActivities throws UNAUTHENTICATED when no principal is found`() {
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$EVENT_GROUP_NAME/eventGroupActivities/123"
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchUpdateEventGroupActivities(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$EVENT_GROUP_NAME/eventGroupActivities/123"
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$EVENT_GROUP_NAME/eventGroupActivities/123"
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(makeDataProvider(DATA_PROVIDER_EXTERNAL_ID + 1)) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when parent is invalid`() {
    val request = batchUpdateEventGroupActivitiesRequest { parent = "invalid" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when chid event group activity is unspecified`() {
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += UpdateEventGroupActivityRequest.getDefaultInstance()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when child event group activity name is invalid`() {
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity { name = "invalid" }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when child and parent EventGroup does not match`() {
    val otherEventGroupName = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAJs"
    val request = batchUpdateEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = "$otherEventGroupName/eventGroupActivities/123"
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity returns Empty`() {
    val activityDate = date {
      year = 2023
      month = 10
      day = 10
    }
    val request = deleteEventGroupActivityRequest {
      name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.deleteEventGroupActivity(request) }
      }

    verifyProtoArgument(
        internalServiceMock,
        EventGroupActivitiesCoroutineImplBase::deleteEventGroupActivity,
      )
      .isEqualTo(
        internalDeleteEventGroupActivityRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalEventGroupActivityId = activityDate
        }
      )

    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when name is empty`() {
    val request = deleteEventGroupActivityRequest { name = "" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteEventGroupActivity(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when name is invalid`() {
    val request = deleteEventGroupActivityRequest { name = "invalid" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteEventGroupActivity(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when date in name is invalid`() {
    val request = deleteEventGroupActivityRequest {
      name = "$EVENT_GROUP_NAME/eventGroupActivities/2013-1-2"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteEventGroupActivity(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteEventGroupActivityRequest {
      name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.deleteEventGroupActivity(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteEventGroupActivity throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = deleteEventGroupActivityRequest {
      name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.deleteEventGroupActivity(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteEventGroupActivity throws PERMISSION_DENIED when principal is wrong`() {
    val request = deleteEventGroupActivityRequest {
      name = "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(makeDataProvider(DATA_PROVIDER_EXTERNAL_ID + 1)) {
          runBlocking { service.deleteEventGroupActivity(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchDeleteEventGroupActivities returns Empty`() {
    val activityDate1 = date {
      year = 2023
      month = 10
      day = 10
    }
    val activityDate2 = date {
      year = 2023
      month = 10
      day = 11
    }
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-11"
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchDeleteEventGroupActivities(request) }
      }

    verifyProtoArgument(
        internalServiceMock,
        EventGroupActivitiesCoroutineImplBase::batchDeleteEventGroupActivities,
      )
      .isEqualTo(
        internalBatchDeleteEventGroupActivitiesRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalEventGroupActivityIds += activityDate1
          externalEventGroupActivityIds += activityDate2
        }
      )

    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when parent is missing`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when parent is invalid`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = "invalid"
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when the EventGroup component of child parent mismatch`() {
    val otherEventGroupName = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAJs"
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$otherEventGroupName/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when a child name is empty`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
      names += ""
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when a child name is invalid`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
      names += "invalid"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when date in name is invalid`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
      names += "$EVENT_GROUP_NAME/eventGroupActivities/not-a-date"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws UNAUTHENTICATED when no principal is found`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchDeleteEventGroupActivities(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws PERMISSION_DENIED when principal is wrong`() {
    val request = batchDeleteEventGroupActivitiesRequest {
      parent = EVENT_GROUP_NAME
      names += "$EVENT_GROUP_NAME/eventGroupActivities/2023-10-10"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(makeDataProvider(DATA_PROVIDER_EXTERNAL_ID + 1)) {
          runBlocking { service.batchDeleteEventGroupActivities(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  private fun createInternalEventGroupActivity(activityDate: Date) = internalEventGroupActivity {
    externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
    externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
    date = date {
      year = activityDate.year
      month = activityDate.month
      day = activityDate.day
    }
  }
}
