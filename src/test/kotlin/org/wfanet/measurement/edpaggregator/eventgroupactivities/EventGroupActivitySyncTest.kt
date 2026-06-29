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

package org.wfanet.measurement.edpaggregator.eventgroupactivities

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Empty
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration as JavaDuration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.check
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.BatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.listEventGroupActivitiesResponse
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler

@RunWith(JUnit4::class)
class EventGroupActivitySyncTest {

  private val activitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { listEventGroupActivities(any<ListEventGroupActivitiesRequest>()) }
      .thenReturn(listEventGroupActivitiesResponse {})
    onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
      .thenReturn(batchUpdateEventGroupActivitiesResponse {})
    onBlocking { batchDeleteEventGroupActivities(any<BatchDeleteEventGroupActivitiesRequest>()) }
      .thenReturn(Empty.getDefaultInstance())
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(activitiesServiceMock) }

  private val activitiesStub: EventGroupActivitiesCoroutineStub by lazy {
    EventGroupActivitiesCoroutineStub(grpcTestServerRule.channel)
  }

  private fun newThrottler(): MinimumIntervalThrottler =
    MinimumIntervalThrottler(Clock.systemUTC(), JavaDuration.ofMillis(1))

  private fun instant(date: String): Instant =
    LocalDate.parse(date).atStartOfDay(ZoneOffset.UTC).toInstant()

  private fun spotRecord(eventGroup: String, date: String): SpotRecord =
    SpotRecord("$DATA_PROVIDER/eventGroups/$eventGroup", instant(date))

  private fun protoDate(dateString: String) =
    LocalDate.parse(dateString).let {
      date {
        year = it.year
        month = it.monthValue
        day = it.dayOfMonth
      }
    }

  private fun stubExistingDates(eventGroup: String, dates: List<String>) {
    wheneverBlocking { activitiesServiceMock.listEventGroupActivities(any()) }
      .thenReturn(
        listEventGroupActivitiesResponse {
          eventGroupActivities +=
            dates.map { dateString ->
              eventGroupActivity {
                name = "$DATA_PROVIDER/eventGroups/$eventGroup/eventGroupActivities/$dateString"
                date = protoDate(dateString)
              }
            }
        }
      )
  }

  private fun newSync(
    throttler: Throttler = newThrottler(),
    maxDeleteFraction: Double = 1.0,
  ): EventGroupActivitySync =
    EventGroupActivitySync(
      eventGroupActivitiesClient = activitiesStub,
      throttler = throttler,
      dataProviderName = DATA_PROVIDER,
      listPageSize = LIST_PAGE_SIZE,
      maxDeleteFraction = maxDeleteFraction,
      // Fast backoff so retry tests do not sleep for the production default.
      retryBackoff = ExponentialBackoff(initialDelay = JavaDuration.ofMillis(1)),
    )

  @Test
  fun `sync creates activities in file but not in Kingdom`() {
    val records =
      listOf(
        spotRecord("eg1", "2026-01-01"),
        spotRecord("eg1", "2026-01-02"),
        spotRecord("eg1", "2026-01-03"),
        spotRecord("eg1", "2026-01-04"),
        spotRecord("eg1", "2026-01-05"),
      )

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesCreated).isEqualTo(5)
    assertThat(result.activitiesDeleted).isEqualTo(0)
    assertThat(result.activitiesUnchanged).isEqualTo(0)
    assertThat(result.errors).isEmpty()
    verifyBlocking(activitiesServiceMock, times(1)) {
      batchUpdateEventGroupActivities(
        check { request ->
          assertThat(request.requestsList).hasSize(5)
          assertThat(request.requestsList.all { it.allowMissing }).isTrue()
          // FIX 10: verify a produced activity name and date, not just list size.
          val first = request.requestsList.first().eventGroupActivity
          assertThat(first.name)
            .isEqualTo("$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-01")
          assertThat(first.date).isEqualTo(protoDate("2026-01-01"))
        }
      )
    }
    verifyBlocking(activitiesServiceMock, times(0)) { batchDeleteEventGroupActivities(any()) }
  }

  @Test
  fun `sync deletes activities in Kingdom but not in file`() {
    stubExistingDates("eg1", listOf("2026-01-01", "2026-01-02", "2026-01-03"))
    // File has only one of the three existing dates.
    val records = listOf(spotRecord("eg1", "2026-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesCreated).isEqualTo(0)
    assertThat(result.activitiesDeleted).isEqualTo(2)
    assertThat(result.activitiesUnchanged).isEqualTo(1)
    verifyBlocking(activitiesServiceMock, times(1)) {
      batchDeleteEventGroupActivities(
        check { request ->
          assertThat(request.namesList).hasSize(2)
          // FIX 10: verify produced resource names.
          assertThat(request.namesList)
            .containsExactly(
              "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-02",
              "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-03",
            )
        }
      )
    }
    verifyBlocking(activitiesServiceMock, times(0)) { batchUpdateEventGroupActivities(any()) }
  }

  @Test
  fun `sync is idempotent - no changes when file matches Kingdom`() {
    val dates = listOf("2026-01-01", "2026-01-02", "2026-01-03")
    stubExistingDates("eg1", dates)
    val records = dates.map { spotRecord("eg1", it) }

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesCreated).isEqualTo(0)
    assertThat(result.activitiesDeleted).isEqualTo(0)
    assertThat(result.activitiesUnchanged).isEqualTo(3)
    verifyBlocking(activitiesServiceMock, times(0)) { batchUpdateEventGroupActivities(any()) }
    verifyBlocking(activitiesServiceMock, times(0)) { batchDeleteEventGroupActivities(any()) }
  }

  @Test
  fun `sync handles multiple EventGroups correctly`() {
    val records =
      listOf(
        spotRecord("eg1", "2026-01-01"),
        spotRecord("eg1", "2026-01-02"),
        spotRecord("eg2", "2026-02-01"),
        spotRecord("eg3", "2026-03-01"),
      )

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.eventGroupsProcessed).isEqualTo(3)
    assertThat(result.activitiesCreated).isEqualTo(4)
    assertThat(result.errors).isEmpty()
    verifyBlocking(activitiesServiceMock, times(3)) { batchUpdateEventGroupActivities(any()) }
  }

  @Test
  fun `sync batches correctly at 1000 activities per request`() {
    val records =
      (0 until 2500).map { i ->
        spotRecord("eg1", LocalDate.of(2026, 1, 1).plusDays(i.toLong()).toString())
      }

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesCreated).isEqualTo(2500)
    val captor = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(activitiesServiceMock, times(3)) {
      batchUpdateEventGroupActivities(captor.capture())
    }
    assertThat(captor.allValues.map { it.requestsList.size })
      .containsExactly(1000, 1000, 500)
      .inOrder()
    // FIX 9: every captured update request sets allowMissing.
    assertThat(captor.allValues.all { req -> req.requestsList.all { it.allowMissing } }).isTrue()
  }

  @Test
  fun `sync chunks deletes into batches of 1000`() {
    val existing =
      (0 until 2500).map { i -> LocalDate.of(2026, 1, 1).plusDays(i.toLong()).toString() }
    stubExistingDates("eg1", existing)
    // File has only one (non-existing) date for eg1, so all 2500 existing dates are deleted
    // -> 3 batchDelete calls of sizes 1000, 1000, 500.
    val records = listOf(spotRecord("eg1", "2000-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesDeleted).isEqualTo(2500)
    val captor = argumentCaptor<BatchDeleteEventGroupActivitiesRequest>()
    verifyBlocking(activitiesServiceMock, times(3)) {
      batchDeleteEventGroupActivities(captor.capture())
    }
    assertThat(captor.allValues.map { it.namesList.size })
      .containsExactly(1000, 1000, 500)
      .inOrder()
  }

  @Test
  fun `sync isolates delete failures across EventGroups`() {
    stubExistingDatesForGroups(
      mapOf(
        "egA" to listOf("2026-01-01", "2026-01-02"),
        "egB" to listOf("2026-02-01", "2026-02-02"),
      )
    )
    wheneverBlocking {
        activitiesServiceMock.batchDeleteEventGroupActivities(
          any<BatchDeleteEventGroupActivitiesRequest>()
        )
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<BatchDeleteEventGroupActivitiesRequest>(0)
        if (request.parent.endsWith("/eventGroups/egA")) {
          throw StatusRuntimeException(Status.INTERNAL)
        }
        Empty.getDefaultInstance()
      }
    // Each group's input has one non-existing date, so all existing dates are to be deleted.
    val records = listOf(spotRecord("egA", "2030-01-01"), spotRecord("egB", "2030-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.errors).hasSize(1)
    assertThat(result.errors.single().eventGroup).isEqualTo("$DATA_PROVIDER/eventGroups/egA")
    // egB deleted its two existing dates; egA failed on its delete batch and is recorded as an
    // error (its create count is not tallied because the exception fires before the counters).
    assertThat(result.eventGroupsProcessed).isEqualTo(1)
    assertThat(result.activitiesDeleted).isEqualTo(2)
    val captor = argumentCaptor<BatchDeleteEventGroupActivitiesRequest>()
    verifyBlocking(activitiesServiceMock, times(2)) {
      batchDeleteEventGroupActivities(captor.capture())
    }
    val egBDelete = captor.allValues.single { it.parent.endsWith("/eventGroups/egB") }
    assertThat(egBDelete.namesList).hasSize(2)
  }

  @Test
  fun `sync follows nextPageToken across multiple list pages`() {
    val page1Dates = listOf("2026-01-01", "2026-01-02")
    val page2Dates = listOf("2026-01-03", "2026-01-04")
    wheneverBlocking { activitiesServiceMock.listEventGroupActivities(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListEventGroupActivitiesRequest>(0)
        if (request.pageToken.isEmpty()) {
          listEventGroupActivitiesResponse {
            eventGroupActivities +=
              page1Dates.map { d ->
                eventGroupActivity {
                  name = "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/$d"
                  date = protoDate(d)
                }
              }
            nextPageToken = "p2"
          }
        } else {
          listEventGroupActivitiesResponse {
            eventGroupActivities +=
              page2Dates.map { d ->
                eventGroupActivity {
                  name = "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/$d"
                  date = protoDate(d)
                }
              }
          }
        }
      }
    // File contains all page1 + page2 dates -> everything unchanged.
    val records = (page1Dates + page2Dates).map { spotRecord("eg1", it) }

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.activitiesUnchanged).isEqualTo(page1Dates.size + page2Dates.size)
    assertThat(result.activitiesCreated).isEqualTo(0)
    assertThat(result.activitiesDeleted).isEqualTo(0)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `sync retries transient UNAVAILABLE failure then succeeds`() {
    var attempts = 0
    wheneverBlocking {
        activitiesServiceMock.batchUpdateEventGroupActivities(
          any<BatchUpdateEventGroupActivitiesRequest>()
        )
      }
      .thenAnswer {
        attempts++
        if (attempts == 1) {
          throw StatusRuntimeException(Status.UNAVAILABLE)
        }
        batchUpdateEventGroupActivitiesResponse {}
      }
    val records = listOf(spotRecord("eg1", "2026-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(attempts).isEqualTo(2)
    assertThat(result.activitiesCreated).isEqualTo(1)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `sync retries transient UNAVAILABLE on list then succeeds`() {
    var listAttempts = 0
    wheneverBlocking {
        activitiesServiceMock.listEventGroupActivities(any<ListEventGroupActivitiesRequest>())
      }
      .thenAnswer {
        listAttempts++
        if (listAttempts == 1) {
          throw StatusRuntimeException(Status.UNAVAILABLE)
        }
        listEventGroupActivitiesResponse {}
      }
    val records = listOf(spotRecord("eg1", "2026-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(listAttempts).isEqualTo(2)
    assertThat(result.activitiesCreated).isEqualTo(1)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `sync retries transient DEADLINE_EXCEEDED failure then succeeds`() {
    var attempts = 0
    wheneverBlocking {
        activitiesServiceMock.batchUpdateEventGroupActivities(
          any<BatchUpdateEventGroupActivitiesRequest>()
        )
      }
      .thenAnswer {
        attempts++
        if (attempts == 1) {
          throw StatusRuntimeException(Status.DEADLINE_EXCEEDED)
        }
        batchUpdateEventGroupActivitiesResponse {}
      }
    val records = listOf(spotRecord("eg1", "2026-01-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(attempts).isEqualTo(2)
    assertThat(result.activitiesCreated).isEqualTo(1)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `sync skips deletes when delete fraction exceeds max`() {
    val existing = (1..10).map { LocalDate.of(2026, 1, it).toString() }
    stubExistingDates("eg1", existing)
    // File keeps only 2 of the 10 existing dates -> 8/10 = 0.8 > 0.5.
    val records = existing.take(2).map { spotRecord("eg1", it) }

    val result = runBlocking { newSync(maxDeleteFraction = 0.5).sync(records) }

    assertThat(result.activitiesDeleted).isEqualTo(0)
    assertThat(result.errors).hasSize(1)
    assertThat(result.errors.single().eventGroup).isEqualTo("$DATA_PROVIDER/eventGroups/eg1")
    assertThat(result.errors.single().message).contains("max delete fraction")
    verifyBlocking(activitiesServiceMock, times(0)) { batchDeleteEventGroupActivities(any()) }
  }

  @Test
  fun `sync validates DataProvider and rejects mismatched records`() {
    val records =
      listOf(
        spotRecord("eg1", "2026-01-01"),
        SpotRecord("dataProviders/other/eventGroups/eg1", instant("2026-01-02")),
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { runBlocking { newSync().sync(records) } }

    assertThat(exception).hasMessageThat().contains("dataProviders/other/eventGroups/eg1")
  }

  @Test
  fun `sync continues on partial failure and reports errors`() {
    wheneverBlocking {
        activitiesServiceMock.batchUpdateEventGroupActivities(
          any<BatchUpdateEventGroupActivitiesRequest>()
        )
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<BatchUpdateEventGroupActivitiesRequest>(0)
        if (request.parent.endsWith("/eventGroups/egA")) {
          throw StatusRuntimeException(Status.INTERNAL)
        }
        batchUpdateEventGroupActivitiesResponse {}
      }
    val records = listOf(spotRecord("egA", "2026-01-01"), spotRecord("egB", "2026-02-01"))

    val result = runBlocking { newSync().sync(records) }

    assertThat(result.errors).hasSize(1)
    assertThat(result.errors.single().eventGroup).isEqualTo("$DATA_PROVIDER/eventGroups/egA")
    assertThat(result.eventGroupsProcessed).isEqualTo(1)
    assertThat(result.activitiesCreated).isEqualTo(1)
    // FIX 9: the recorded cause carries the INTERNAL gRPC status. Server-thrown errors arrive on
    // the client as a StatusException (not StatusRuntimeException) over a real channel.
    val cause = result.errors.single().cause
    assertThat(cause).isInstanceOf(StatusException::class.java)
    assertThat((cause as StatusException).status.code).isEqualTo(Status.Code.INTERNAL)
  }

  @Test
  fun `sync dry-run computes counts without making mutating calls`() {
    stubExistingDates("eg1", listOf("2026-01-01", "2026-01-02"))
    // One unchanged (2026-01-01), one to delete (2026-01-02), one to create (2026-01-03).
    val records = listOf(spotRecord("eg1", "2026-01-01"), spotRecord("eg1", "2026-01-03"))

    val result = runBlocking { newSync().sync(records, dryRun = true) }

    assertThat(result.activitiesCreated).isEqualTo(1)
    assertThat(result.activitiesDeleted).isEqualTo(1)
    assertThat(result.activitiesUnchanged).isEqualTo(1)
    verifyBlocking(activitiesServiceMock, times(0)) { batchUpdateEventGroupActivities(any()) }
    verifyBlocking(activitiesServiceMock, times(0)) { batchDeleteEventGroupActivities(any()) }
    verifyBlocking(activitiesServiceMock, org.mockito.kotlin.atLeastOnce()) {
      listEventGroupActivities(any())
    }
  }

  @Test
  fun `sync throttles between batches`() {
    var onReadyCount = 0
    val delegate = newThrottler()
    val throttler =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T {
          onReadyCount++
          return delegate.onReady(block)
        }
      }
    val records = listOf(spotRecord("eg1", "2026-01-01"))

    runBlocking { newSync(throttler).sync(records) }

    // At least one list call plus one batch update call.
    assertThat(onReadyCount).isAtLeast(2)
  }

  /** Stubs distinct existing dates per EventGroup, keyed on the request parent. */
  private fun stubExistingDatesForGroups(datesByGroup: Map<String, List<String>>) {
    wheneverBlocking { activitiesServiceMock.listEventGroupActivities(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListEventGroupActivitiesRequest>(0)
        val group = request.parent.substringAfterLast("/eventGroups/")
        val dates = datesByGroup[group] ?: emptyList()
        listEventGroupActivitiesResponse {
          eventGroupActivities +=
            dates.map { d ->
              eventGroupActivity {
                name = "$DATA_PROVIDER/eventGroups/$group/eventGroupActivities/$d"
                date = protoDate(d)
              }
            }
        }
      }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val LIST_PAGE_SIZE = 50
  }
}
