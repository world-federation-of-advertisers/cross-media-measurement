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
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
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
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesRequest as internalBatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesResponse as internalBatchUpdateEventGroupActivitiesResponse
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
