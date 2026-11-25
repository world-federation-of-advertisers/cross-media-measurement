// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerExceptionFactory
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.ReadEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.ReadEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.readEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.EventGroupAndHelperServices
import org.wfanet.measurement.kingdom.service.internal.testing.EventGroupsServiceTest
import org.wfanet.measurement.kingdom.service.internal.testing.Population

@RunWith(JUnit4::class)
class SpannerEventGroupsServiceTest : EventGroupsServiceTest<SpannerEventGroupsService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)
  private val clock = Clock.systemUTC()
  private lateinit var syncService: SpannerEventGroupsService

  override fun newServices(
    idGenerator: IdGenerator
  ): EventGroupAndHelperServices<SpannerEventGroupsService> {
    val spannerServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient).buildDataServices()

    syncService = spannerServices.eventGroupsService as SpannerEventGroupsService
    return EventGroupAndHelperServices(
      spannerServices.eventGroupsService as SpannerEventGroupsService,
      spannerServices.measurementConsumersService,
      spannerServices.dataProvidersService,
      spannerServices.accountsService,
      spannerServices.eventGroupActivitiesService,
    )
  }

  @Test
  fun `read empty inventory returns actual reusable read time`(): Unit = runBlocking {
    val first = syncService.readEventGroups(readEventGroupsRequest { limit = 1 })
    assertThat(first.eventGroupsList).isEmpty()
    assertThat(first.hasReadTime()).isTrue()
    assertThat(Timestamps.isValid(first.readTime)).isTrue()
    val repeated =
      syncService.readEventGroups(
        readEventGroupsRequest {
          limit = 1
          readTime = first.readTime
        }
      )
    assertThat(repeated.readTime).isEqualTo(first.readTime)
  }

  @Test
  fun `read inventory includes all providers consumers types and deleted rows`(): Unit =
    runBlocking {
      val first = createSyncGroup("campaign")
      val second = createSyncGroup("client-account")
      syncService.deleteEventGroup(
        deleteEventGroupRequest {
          externalDataProviderId = second.externalDataProviderId
          externalEventGroupId = second.externalEventGroupId
        }
      )
      val response = syncService.readEventGroups(readEventGroupsRequest { limit = 10 })
      assertThat(response.eventGroupsList.map { it.externalDataProviderId })
        .containsExactly(first.externalDataProviderId, second.externalDataProviderId)
      assertThat(response.eventGroupsList.map { it.externalMeasurementConsumerId })
        .containsExactly(first.externalMeasurementConsumerId, second.externalMeasurementConsumerId)
      assertThat(response.eventGroupsList.map { it.state })
        .containsExactly(EventGroup.State.ACTIVE, EventGroup.State.DELETED)
      assertThat(response.eventGroupsList.map { it.entityKey.entityType })
        .containsExactly("campaign", "client-account")
      assertThat(
          response.eventGroupsList.all { Timestamps.compare(it.updateTime, response.readTime) <= 0 }
        )
        .isTrue()
    }

  @Test
  fun `delta excludes watermark and returns latest update and deletion`(): Unit = runBlocking {
    val first = createSyncGroup("campaign")
    val second = createSyncGroup("campaign")
    val snapshot = syncService.readEventGroups(readEventGroupsRequest { limit = 10 })
    syncService.updateEventGroup(
      updateEventGroupRequest { eventGroup = first.copy { providedEventGroupId = "new-reference" } }
    )
    syncService.deleteEventGroup(
      deleteEventGroupRequest {
        externalDataProviderId = second.externalDataProviderId
        externalEventGroupId = second.externalEventGroupId
      }
    )
    val delta =
      syncService.readEventGroups(
        readEventGroupsRequest {
          updatedAfter = snapshot.readTime
          limit = 10
        }
      )
    assertThat(delta.eventGroupsList).hasSize(2)
    assertThat(delta.eventGroupsList.map { it.state })
      .containsExactly(EventGroup.State.ACTIVE, EventGroup.State.DELETED)
    assertThat(
        delta.eventGroupsList
          .first { it.externalEventGroupId == first.externalEventGroupId }
          .providedEventGroupId
      )
      .isEqualTo("new-reference")
    val empty =
      syncService.readEventGroups(
        readEventGroupsRequest {
          updatedAfter = delta.readTime
          limit = 10
        }
      )
    assertThat(empty.eventGroupsList).isEmpty()
  }

  @Test
  fun `equal commit timestamps page using unique keys without omissions`(): Unit = runBlocking {
    val seed = createSyncGroup("campaign")
    val batch =
      syncService.batchCreateEventGroups(
        batchCreateEventGroupsRequest {
          externalDataProviderId = seed.externalDataProviderId
          requests +=
            (1..7).map { id ->
              createEventGroupRequest {
                eventGroup =
                  seed.copy {
                    clearExternalEventGroupId()
                    clearCreateTime()
                    clearUpdateTime()
                    providedEventGroupId = "batch-$id"
                    entityKey = entityKey.copy { entityId = "batch-$id" }
                  }
              }
            }
        }
      )
    assertThat(batch.eventGroupsList.map { it.updateTime }.distinct()).hasSize(1)
    val all = mutableListOf<EventGroup>()
    var request = readEventGroupsRequest { limit = 2 }
    do {
      val page = syncService.readEventGroups(request)
      all += page.eventGroupsList
      val last = page.eventGroupsList.lastOrNull() ?: break
      request = readEventGroupsRequest {
        limit = 2
        readTime = page.readTime
        after = cursor(last)
      }
    } while (true)
    assertThat(all.map { it.externalEventGroupId })
      .containsExactlyElementsIn(
        listOf(seed.externalEventGroupId) + batch.eventGroupsList.map { it.externalEventGroupId }
      )
    assertThat(all.map { it.externalEventGroupId }.distinct()).hasSize(8)
  }

  @Test
  fun `snapshot pages retain old versions when unread and returned rows change`(): Unit =
    runBlocking {
      val first = createSyncGroup("campaign")
      val second = createSyncGroup("campaign")
      val page = syncService.readEventGroups(readEventGroupsRequest { limit = 1 })
      assertThat(page.eventGroupsList.single().externalEventGroupId)
        .isEqualTo(first.externalEventGroupId)
      syncService.updateEventGroup(
        updateEventGroupRequest {
          eventGroup = first.copy { providedEventGroupId = "changed-after-read" }
        }
      )
      syncService.deleteEventGroup(
        deleteEventGroupRequest {
          externalDataProviderId = second.externalDataProviderId
          externalEventGroupId = second.externalEventGroupId
        }
      )
      val third = createSyncGroup("client-account")
      val remainder =
        syncService.readEventGroups(
          readEventGroupsRequest {
            limit = 10
            readTime = page.readTime
            after = cursor(first)
          }
        )
      assertThat(remainder.readTime).isEqualTo(page.readTime)
      assertThat(remainder.eventGroupsList.map { it.externalEventGroupId })
        .containsExactly(second.externalEventGroupId)
      assertThat(remainder.eventGroupsList.single().state).isEqualTo(EventGroup.State.ACTIVE)
      val delta =
        syncService.readEventGroups(
          readEventGroupsRequest {
            limit = 10
            updatedAfter = page.readTime
          }
        )
      assertThat(delta.eventGroupsList.map { it.externalEventGroupId })
        .containsExactly(
          first.externalEventGroupId,
          second.externalEventGroupId,
          third.externalEventGroupId,
        )
    }

  @Test
  fun `read requires bounded positive limit`(): Unit = runBlocking {
    for (size in listOf(-1, 0, 2002)) {
      val error =
        assertFailsWith<StatusRuntimeException> {
          syncService.readEventGroups(readEventGroupsRequest { limit = size })
        }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }
  }

  @Test
  fun `read rejects cursor without snapshot`(): Unit = runBlocking {
    val group = createSyncGroup("campaign")
    val error =
      assertFailsWith<StatusRuntimeException> {
        syncService.readEventGroups(
          readEventGroupsRequest {
            limit = 1
            after = cursor(group)
          }
        )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `read rejects wrong cursor timestamp and unknown cursor key`(): Unit = runBlocking {
    val group = createSyncGroup("campaign")
    val page = syncService.readEventGroups(readEventGroupsRequest { limit = 1 })
    for (after in
      listOf(
        cursor(group).copy {
          updateTime =
            Timestamps.subtract(updateTime, com.google.protobuf.util.Durations.fromSeconds(1))
        },
        cursor(group).copy { externalEventGroupId = 999L },
      )) {
      val error =
        assertFailsWith<StatusRuntimeException> {
          syncService.readEventGroups(
            readEventGroupsRequest {
              limit = 1
              readTime = page.readTime
              this.after = after
            }
          )
        }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }
  }

  @Test
  fun `read rejects checkpoint later than snapshot`(): Unit = runBlocking {
    val page = syncService.readEventGroups(readEventGroupsRequest { limit = 1 })
    val error =
      assertFailsWith<StatusRuntimeException> {
        syncService.readEventGroups(
          readEventGroupsRequest {
            limit = 1
            readTime = page.readTime
            updatedAfter =
              Timestamps.add(page.readTime, com.google.protobuf.util.Durations.fromSeconds(1))
          }
        )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `native Spanner failures preserve retryability without database details`(): Unit =
    runBlocking {
      for (code in
        listOf(
          ErrorCode.UNAVAILABLE,
          ErrorCode.DEADLINE_EXCEEDED,
          ErrorCode.CANCELLED,
          ErrorCode.ABORTED,
          ErrorCode.RESOURCE_EXHAUSTED,
        )) {
        val client: AsyncDatabaseClient = mock()
        whenever(client.readOnlyTransaction(any()))
          .thenThrow(SpannerExceptionFactory.newSpannerException(code, "private schema detail"))
        val service =
          SpannerEventGroupsService(
            idGenerator,
            client,
            Duration.ofSeconds(1),
            EmptyCoroutineContext,
          )
        val error =
          assertFailsWith<StatusRuntimeException> {
            service.readEventGroups(readEventGroupsRequest { limit = 1 })
          }
        assertThat(error.status.code.name).isEqualTo(code.name)
        assertThat(error.status.description).doesNotContain("private schema detail")
      }
    }

  @Test
  fun `schema unavailable at old snapshot requires a fresh snapshot`(): Unit = runBlocking {
    val client: AsyncDatabaseClient = mock()
    whenever(client.readOnlyTransaction(any()))
      .thenThrow(
        SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "private schema detail",
        )
      )
    val service =
      SpannerEventGroupsService(idGenerator, client, Duration.ofSeconds(1), EmptyCoroutineContext)
    val error =
      assertFailsWith<StatusRuntimeException> {
        service.readEventGroups(
          readEventGroupsRequest {
            limit = 1
            readTime = clock.instant().toProtoTime()
          }
        )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(error.errorInfo?.reason).isEqualTo("READ_TIME_EXPIRED")
    val freshError =
      assertFailsWith<StatusRuntimeException> {
        service.readEventGroups(readEventGroupsRequest { limit = 1 })
      }
    assertThat(freshError.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(freshError.status.description).doesNotContain("private schema detail")
  }

  private suspend fun createSyncGroup(type: String): EventGroup {
    val population = Population(clock, idGenerator)
    val mc = population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dp = population.createDataProvider(dataProvidersService)
    return syncService.createEventGroup(
      createEventGroupRequest {
        eventGroup = eventGroup {
          externalDataProviderId = dp.externalDataProviderId
          externalMeasurementConsumerId = mc.externalMeasurementConsumerId
          entityKey =
            org.wfanet.measurement.internal.kingdom.EventGroupKt.entityKey {
              entityType = type
              entityId = "entity"
            }
          details = eventGroupDetails { apiVersion = Version.V2_ALPHA.string }
        }
      }
    )
  }

  private fun cursor(group: EventGroup): ReadEventGroupsRequest.After =
    ReadEventGroupsRequestKt.after {
      updateTime = group.updateTime
      externalDataProviderId = group.externalDataProviderId
      externalEventGroupId = group.externalEventGroupId
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
