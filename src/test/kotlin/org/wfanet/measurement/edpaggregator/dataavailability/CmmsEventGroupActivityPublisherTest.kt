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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.Timestamps
import com.google.type.date
import com.google.type.interval
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt.entityKey as cmmsEntityKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata

@RunWith(JUnit4::class)
class CmmsEventGroupActivityPublisherTest {
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenReturn(listEventGroupsResponse {})
  }

  private val eventGroupActivitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
      .thenReturn(batchUpdateEventGroupActivitiesResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(eventGroupsServiceMock)
    addService(eventGroupActivitiesServiceMock)
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupActivitiesStub: EventGroupActivitiesCoroutineStub by lazy {
    EventGroupActivitiesCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `publish writes every market date touched by interval`() = runBlocking {
    wheneverBlocking { eventGroupsServiceMock.listEventGroups(any()) }
      .thenReturn(
        listEventGroupsResponse {
          eventGroups +=
            listOf(
              eventGroup {
                name = "$DATA_PROVIDER/eventGroups/eg1"
                entityKey = cmmsEntityKey {
                  entityType = "campaign"
                  entityId = "123"
                }
              },
              eventGroup {
                name = "$DATA_PROVIDER/eventGroups/eg2"
                entityKey = cmmsEntityKey {
                  entityType = "campaign"
                  entityId = "123"
                }
              },
            )
        }
      )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T07:30:00Z")
        endTime = Timestamps.parse("2026-01-02T08:30:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
      entityKeys += entityKey {
        entityType = "ad_group"
        entityId = "456"
      }
    }

    val published = newPublisher(ZoneId.of("America/Los_Angeles")).publish(listOf(metadata))

    assertThat(published).isEqualTo(4)
    val listRequest = argumentCaptor<ListEventGroupsRequest>()
    verifyBlocking(eventGroupsServiceMock) { listEventGroups(listRequest.capture()) }
    assertThat(listRequest.firstValue.filter.entityTypeInList).containsExactly("campaign")

    val updateRequests = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock, times(2)) {
      batchUpdateEventGroupActivities(updateRequests.capture())
    }
    assertThat(updateRequests.allValues.map { it.parent })
      .containsExactly("$DATA_PROVIDER/eventGroups/eg1", "$DATA_PROVIDER/eventGroups/eg2")
    for (request in updateRequests.allValues) {
      assertThat(request.requestsList.map { it.eventGroupActivity.date })
        .containsExactly(
          date {
            year = 2026
            month = 1
            day = 1
          },
          date {
            year = 2026
            month = 1
            day = 2
          },
        )
        .inOrder()
      assertThat(request.requestsList.all { it.allowMissing }).isTrue()
    }
  }

  @Test
  fun `publish includes both endpoint dates at market midnight`() = runBlocking {
    wheneverBlocking { eventGroupsServiceMock.listEventGroups(any()) }
      .thenReturn(
        listEventGroupsResponse {
          eventGroups += eventGroup {
            name = "$DATA_PROVIDER/eventGroups/eg1"
            entityKey = cmmsEntityKey {
              entityType = "campaign"
              entityId = "123"
            }
          }
        }
      )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-01T08:00:00Z")
        endTime = Timestamps.parse("2026-01-02T08:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }

    val published = newPublisher(ZoneId.of("America/Los_Angeles")).publish(listOf(metadata))

    assertThat(published).isEqualTo(2)
    val request = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      batchUpdateEventGroupActivities(request.capture())
    }
    assertThat(request.firstValue.requestsList.map { it.eventGroupActivity.date })
      .containsExactly(
        date {
          year = 2026
          month = 1
          day = 1
        },
        date {
          year = 2026
          month = 1
          day = 2
        },
      )
      .inOrder()
  }

  @Test
  fun `publish fails when configured entity key has no EventGroup`() = runBlocking {
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-01T00:00:00Z")
        endTime = Timestamps.parse("2026-01-02T00:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "missing"
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        newPublisher(ZoneId.of("UTC")).publish(listOf(metadata))
      }

    assertThat(exception).hasMessageThat().contains("did not match an EventGroup")
    verifyBlocking(eventGroupActivitiesServiceMock, never()) {
      batchUpdateEventGroupActivities(any())
    }
  }

  private fun newPublisher(timeZone: ZoneId): CmmsEventGroupActivityPublisher =
    CmmsEventGroupActivityPublisher(
      eventGroupsStub,
      eventGroupActivitiesStub,
      DATA_PROVIDER,
      setOf("campaign"),
      maxConcurrentRequests = 2,
      timeZone,
    )

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
  }
}
