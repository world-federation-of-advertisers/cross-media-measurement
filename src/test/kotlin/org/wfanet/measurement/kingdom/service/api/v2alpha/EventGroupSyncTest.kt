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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.aead.PredefinedAeadParameters
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.SyncEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.SyncEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.syncEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.asRuntimeException
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReadEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.ReadEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.readEventGroupsResponse

@RunWith(JUnit4::class)
class EventGroupSyncTest {
  private val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
  private val store = VersionedEventGroups(clock)
  @get:Rule val grpc = GrpcTestServerRule { addService(store) }
  private val operator = AccountPrincipal(AccountKey("operator"))
  private val secondOperator = AccountPrincipal(AccountKey("second-operator"))
  private lateinit var service: EventGroupsService
  private lateinit var options: EventGroupSync.Options
  private val base = syncEventGroupsRequest { parent = "dataProviders/-" }

  @Before
  fun init() {
    AeadConfig.register()
    options =
      EventGroupSync.Options(
        setOf(operator.resourceKey.toName(), secondOperator.resourceKey.toName()),
        KeysetHandle.generateNew(PredefinedAeadParameters.AES128_GCM)
          .getPrimitive(RegistryConfiguration.get(), Aead::class.java),
        Duration.ofMinutes(15),
        Duration.ofDays(30),
        clock,
      )
    service = EventGroupsService(EventGroupsCoroutineStub(grpc.channel), options)
  }

  @Test
  fun `first sync includes all providers consumers types and deleted states`(): Unit = runBlocking {
    store.put(group(1, 11, "campaign", InternalEventGroup.State.ACTIVE))
    store.put(group(2, 12, "client-account", InternalEventGroup.State.DELETED))
    val result = sync(base)
    assertThat(result.eventGroupsList.map { it.measurementConsumer }.distinct()).hasSize(2)
    assertThat(result.eventGroupsList.map { it.name })
      .containsExactly(
        "dataProviders/AAAAAAAAAAE/eventGroups/AAAAAAAAAAs",
        "dataProviders/AAAAAAAAAAI/eventGroups/AAAAAAAAAAw",
      )
    assertThat(result.eventGroupsList.map { it.state })
      .containsExactly(EventGroup.State.ACTIVE, EventGroup.State.DELETED)
    assertThat(result.eventGroupsList.map { it.entityKey.entityType })
      .containsExactly("campaign", "client-account")
    assertThat(result.nextPageToken).isEmpty()
    assertThat(result.nextSyncToken).isNotEmpty()
    assertThat(result.readTime).isEqualTo(clock.instant().toProtoTime())
  }

  @Test
  fun `empty inventory and empty delta still advance checkpoint`(): Unit = runBlocking {
    val inventory = sync(base)
    clock.advance(Duration.ofSeconds(1))
    val delta = sync(base.copy { syncToken = inventory.nextSyncToken })
    assertThat(delta.eventGroupsList).isEmpty()
    assertThat(delta.nextSyncToken).isNotEmpty()
    assertThat(Timestamps.compare(delta.readTime, inventory.readTime)).isGreaterThan(0)
    assertThat(store.requests.last().updatedAfter).isEqualTo(inventory.readTime)
  }

  @Test
  fun `writes during paging appear in next sync while current snapshot remains unchanged`() =
    runBlocking {
      for (id in 1L..3L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
      val first = sync(base.copy { pageSize = 1 })
      assertThat(first.nextSyncToken).isEmpty()
      assertThat(first.nextPageToken).isNotEmpty()
      clock.advance(Duration.ofSeconds(1))
      store.put(group(1, 1, "campaign", InternalEventGroup.State.DELETED))
      store.put(group(1, 2, "campaign", InternalEventGroup.State.DELETED))
      store.put(group(2, 4, "client-account", InternalEventGroup.State.ACTIVE))
      val last =
        sync(
          base.copy {
            pageToken = first.nextPageToken
            pageSize = 2
          }
        )
      assertThat(last.readTime).isEqualTo(first.readTime)
      assertThat(last.eventGroupsList.map { it.state })
        .containsExactly(EventGroup.State.ACTIVE, EventGroup.State.ACTIVE)
      assertThat(last.eventGroupsList.map { it.name }).hasSize(2)
      assertThat(last.nextPageToken).isEmpty()
      val delta = sync(base.copy { syncToken = last.nextSyncToken })
      assertThat(delta.eventGroupsList.map { it.state })
        .containsExactly(
          EventGroup.State.DELETED,
          EventGroup.State.DELETED,
          EventGroup.State.ACTIVE,
        )
      assertThat(delta.eventGroupsList.map { it.name }).hasSize(3)
      assertThat(store.requests[1].readTime).isEqualTo(first.readTime)
    }

  @Test
  fun `delta returns only latest version and excludes exact previous watermark`(): Unit =
    runBlocking {
      store.put(group(1, 1, "campaign", InternalEventGroup.State.ACTIVE))
      val first = sync(base)
      clock.advance(Duration.ofSeconds(1))
      store.put(group(1, 2, "campaign", InternalEventGroup.State.ACTIVE))
      clock.advance(Duration.ofSeconds(1))
      store.put(group(1, 2, "campaign", InternalEventGroup.State.DELETED))
      val result = sync(base.copy { syncToken = first.nextSyncToken })
      assertThat(result.eventGroupsList).hasSize(1)
      assertThat(result.eventGroupsList.single().state).isEqualTo(EventGroup.State.DELETED)
    }

  @Test
  fun `equal update timestamps page without skipping groups`(): Unit = runBlocking {
    for (provider in 1L..3L) for (id in 1L..3L) store.put(
      group(provider, id, "campaign", InternalEventGroup.State.ACTIVE)
    )
    val names = mutableListOf<String>()
    var request = base.copy { pageSize = 2 }
    do {
      val response = sync(request)
      names += response.eventGroupsList.map { it.name }
      request = request.copy { pageToken = response.nextPageToken }
    } while (request.pageToken.isNotEmpty())
    assertThat(names).hasSize(9)
    assertThat(names.distinct()).hasSize(9)
  }

  @Test
  fun `retry page is identical and another server can continue it`(): Unit = runBlocking {
    for (id in 1L..3L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
    val first = sync(base.copy { pageSize = 1 })
    val request =
      base.copy {
        pageToken = first.nextPageToken
        pageSize = 1
      }
    val second = sync(request)
    service = EventGroupsService(EventGroupsCoroutineStub(grpc.channel), options)
    val retried = sync(request)
    assertThat(retried.eventGroupsList).isEqualTo(second.eventGroupsList)
    assertThat(retried.readTime).isEqualTo(second.readTime)
  }

  @Test
  fun `expired page restarts from completed checkpoint and safely replays changes`(): Unit =
    runBlocking {
      val checkpoint = sync(base)
      clock.advance(Duration.ofSeconds(1))
      for (id in 1L..3L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
      val deltaRequest =
        base.copy {
          syncToken = checkpoint.nextSyncToken
          pageSize = 1
        }
      val first = sync(deltaRequest)
      clock.advance(Duration.ofMinutes(15))
      val error =
        assertFailsWith<StatusRuntimeException> {
          sync(deltaRequest.copy { pageToken = first.nextPageToken })
        }
      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(error.errorInfo?.reason).isEqualTo("SYNC_PAGE_TOKEN_EXPIRED")
      store.put(group(1, 1, "campaign", InternalEventGroup.State.DELETED))
      val restarted = sync(base.copy { syncToken = checkpoint.nextSyncToken })
      assertThat(restarted.eventGroupsList).hasSize(3)
      assertThat(restarted.eventGroupsList.map { it.state }).contains(EventGroup.State.DELETED)
    }

  @Test
  fun `page expiry does not extend when following another page`(): Unit = runBlocking {
    for (id in 1L..4L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
    val first = sync(base.copy { pageSize = 1 })
    clock.advance(Duration.ofMinutes(14))
    val second =
      sync(
        base.copy {
          pageSize = 1
          pageToken = first.nextPageToken
        }
      )
    clock.advance(Duration.ofMinutes(1))
    val error =
      assertFailsWith<StatusRuntimeException> {
        sync(base.copy { pageToken = second.nextPageToken })
      }
    assertThat(error.errorInfo?.reason).isEqualTo("SYNC_PAGE_TOKEN_EXPIRED")
  }

  @Test
  fun `checkpoint expiry starts at snapshot time rather than page completion`(): Unit =
    runBlocking {
      for (id in 1L..2L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
      val first = sync(base.copy { pageSize = 1 })
      clock.advance(Duration.ofMinutes(14))
      val last = sync(base.copy { pageToken = first.nextPageToken })
      clock.advance(Duration.ofDays(30).minusMinutes(14))
      val error =
        assertFailsWith<StatusRuntimeException> {
          sync(base.copy { syncToken = last.nextSyncToken })
        }
      assertThat(error.errorInfo?.reason).isEqualTo("SYNC_TOKEN_EXPIRED")
    }

  @Test
  fun `expired completed checkpoint requires full inventory`(): Unit = runBlocking {
    val result = sync(base)
    clock.advance(Duration.ofDays(30))
    val error =
      assertFailsWith<StatusRuntimeException> {
        sync(base.copy { syncToken = result.nextSyncToken })
      }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(error.errorInfo?.reason).isEqualTo("SYNC_TOKEN_EXPIRED")
  }

  @Test
  fun `database snapshot expiry has public recovery reason`(): Unit = runBlocking {
    for (id in 1L..2L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
    val first = sync(base.copy { pageSize = 1 })
    store.failure =
      Status.FAILED_PRECONDITION.asRuntimeException(
        errorInfo {
          domain = "internal.kingdom.wfanet.org"
          reason = "READ_TIME_EXPIRED"
        }
      )
    val error =
      assertFailsWith<StatusRuntimeException> {
        sync(base.copy { pageToken = first.nextPageToken })
      }
    assertThat(error.errorInfo?.reason).isEqualTo("SYNC_PAGE_TOKEN_EXPIRED")
  }

  @Test
  fun `ordinary principals cannot sync any global rows`(): Unit = runBlocking {
    for (name in
      listOf(
        "dataProviders/one",
        "measurementConsumers/one",
        "modelProviders/one",
        "duchies/one",
        "accounts/one",
      )) {
      val principal = requireNotNull(MeasurementPrincipal.fromName(name))
      val error =
        assertFailsWith<StatusRuntimeException> {
          withPrincipal(principal) { service.syncEventGroups(base) }
        }
      assertThat(error.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }
    assertThat(store.requests).isEmpty()
  }

  @Test
  fun `anonymous request is unauthenticated`(): Unit = runBlocking {
    val error = assertFailsWith<StatusRuntimeException> { service.syncEventGroups(base) }
    assertThat(error.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(store.requests).isEmpty()
  }

  @Test
  fun `sync disabled without operator configuration`(): Unit = runBlocking {
    service = EventGroupsService(EventGroupsCoroutineStub(grpc.channel), null)
    val error = assertFailsWith<StatusRuntimeException> { sync(base) }
    assertThat(error.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(store.requests).isEmpty()
  }

  @Test
  fun `malformed and altered tokens are rejected before reading`(): Unit = runBlocking {
    val valid = sync(base).nextSyncToken
    store.requests.clear()
    for (token in listOf("%", "a", "abcd", valid.dropLast(8) + "AAAAAAAA", "x".repeat(4097))) {
      val error = assertFailsWith<StatusRuntimeException> { sync(base.copy { syncToken = token }) }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }
    assertThat(store.requests).isEmpty()
  }

  @Test
  fun `tokens are bound to authenticated caller`(): Unit = runBlocking {
    val token = sync(base).nextSyncToken
    val error =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(secondOperator) { service.syncEventGroups(base.copy { syncToken = token }) }
      }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `page token cannot be used as completed checkpoint`(): Unit = runBlocking {
    for (id in 1L..2L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
    val result = sync(base.copy { pageSize = 1 })
    val error =
      assertFailsWith<StatusRuntimeException> {
        sync(base.copy { syncToken = result.nextPageToken })
      }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `completed checkpoint cannot be used as page token`(): Unit = runBlocking {
    val token = sync(base).nextSyncToken
    val error = assertFailsWith<StatusRuntimeException> { sync(base.copy { pageToken = token }) }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `completed token must remain unchanged while paging`(): Unit = runBlocking {
    val checkpoint = sync(base).nextSyncToken
    clock.advance(Duration.ofSeconds(1))
    for (id in 1L..2L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
    val first =
      sync(
        base.copy {
          syncToken = checkpoint
          pageSize = 1
        }
      )
    val error =
      assertFailsWith<StatusRuntimeException> {
        sync(base.copy { pageToken = first.nextPageToken })
      }
    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `invalid parent and negative size are rejected`(): Unit = runBlocking {
    for (request in
      listOf(
        base.copy { parent = "" },
        base.copy { parent = "measurementConsumers/-" },
        base.copy { pageSize = -1 },
      )) {
      val error = assertFailsWith<StatusRuntimeException> { sync(request) }
      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }
    assertThat(store.requests).isEmpty()
  }

  @Test
  fun `default and maximum page sizes bound reads`(): Unit = runBlocking {
    sync(base)
    sync(base.copy { pageSize = 100000 })
    assertThat(store.requests.map { it.limit }).containsExactly(1001, 2001).inOrder()
  }

  @Test
  fun `backend failures preserve transient codes and hide database details`(): Unit = runBlocking {
    for (code in
      listOf(
        Status.UNAVAILABLE,
        Status.DEADLINE_EXCEEDED,
        Status.CANCELLED,
        Status.ABORTED,
        Status.RESOURCE_EXHAUSTED,
        Status.FAILED_PRECONDITION,
      )) {
      store.failure = code.withDescription("private database detail").asRuntimeException()
      val error = assertFailsWith<StatusRuntimeException> { sync(base) }
      assertThat(error.status.code)
        .isEqualTo(
          if (code.code == Status.Code.FAILED_PRECONDITION) Status.Code.INTERNAL else code.code
        )
      assertThat(error.status.description).doesNotContain("private database")
    }
  }

  @Test
  fun `retry with a smaller page size keeps the snapshot and all remaining groups`(): Unit =
    runBlocking {
      for (id in 1L..3L) store.put(group(1, id, "campaign", InternalEventGroup.State.ACTIVE))
      val first = sync(base.copy { pageSize = 1 })
      val request =
        base.copy {
          pageToken = first.nextPageToken
          pageSize = 2
        }
      val largePage = sync(request)
      clock.advance(Duration.ofSeconds(1))
      store.put(group(1, 2, "campaign", InternalEventGroup.State.DELETED))
      val smallerPage = sync(request.copy { pageSize = 1 })
      val last =
        sync(
          request.copy {
            pageToken = smallerPage.nextPageToken
            pageSize = 1
          }
        )
      assertThat(smallerPage.readTime).isEqualTo(first.readTime)
      assertThat(last.readTime).isEqualTo(first.readTime)
      assertThat(smallerPage.eventGroupsList + last.eventGroupsList)
        .containsExactlyElementsIn(largePage.eventGroupsList)
        .inOrder()
      assertThat(smallerPage.nextSyncToken).isEmpty()
      assertThat(last.nextPageToken).isEmpty()
      assertThat(last.nextSyncToken).isNotEmpty()
      val delta = sync(base.copy { syncToken = last.nextSyncToken })
      assertThat(delta.eventGroupsList.single().state).isEqualTo(EventGroup.State.DELETED)
    }

  private suspend fun sync(request: SyncEventGroupsRequest): SyncEventGroupsResponse =
    withPrincipal(operator) { service.syncEventGroups(request) }

  private fun group(provider: Long, id: Long, entityType: String, state: InternalEventGroup.State) =
    eventGroup {
      externalDataProviderId = provider
      externalEventGroupId = id
      externalMeasurementConsumerId = provider
      updateTime = clock.instant().toProtoTime()
      createTime = updateTime
      this.state = state
      entityKey =
        org.wfanet.measurement.internal.kingdom.EventGroupKt.entityKey {
          this.entityType = entityType
          entityId = "entity-$id"
        }
      details = eventGroupDetails { apiVersion = Version.V2_ALPHA.string }
    }

  private class MutableClock(var now: Instant) : Clock() {
    override fun instant(): Instant = now

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = Clock.fixed(now, zone)

    fun advance(duration: Duration) {
      now = now.plus(duration)
    }
  }

  /** Retains row versions so public paging tests can interleave writes. */
  private class VersionedEventGroups(private val clock: Clock) : EventGroupsCoroutineImplBase() {
    private val versions = mutableListOf<InternalEventGroup>()
    val requests = mutableListOf<ReadEventGroupsRequest>()
    var failure: StatusRuntimeException? = null

    fun put(group: InternalEventGroup) {
      versions += group
    }

    override suspend fun readEventGroups(request: ReadEventGroupsRequest): ReadEventGroupsResponse {
      requests += request
      failure?.let { throw it }
      val readTime = if (request.hasReadTime()) request.readTime else clock.instant().toProtoTime()
      val latest =
        versions
          .filter { Timestamps.compare(it.updateTime, readTime) <= 0 }
          .groupBy { it.externalDataProviderId to it.externalEventGroupId }
          .values
          .map { it.last() }
          .filter {
            !request.hasUpdatedAfter() ||
              Timestamps.compare(it.updateTime, request.updatedAfter) > 0
          }
          .sortedWith(
            compareBy<InternalEventGroup> { it.updateTime.toInstant() }
              .thenBy { it.externalDataProviderId }
              .thenBy { it.externalEventGroupId }
          )
      val cursorIndex =
        if (request.hasAfter())
          latest.indexOfFirst {
            it.externalDataProviderId == request.after.externalDataProviderId &&
              it.externalEventGroupId == request.after.externalEventGroupId
          }
        else -1
      val page = latest.drop(cursorIndex + 1).take(request.limit)
      return readEventGroupsResponse {
        this.readTime = readTime
        eventGroups += page
      }
    }
  }
}
