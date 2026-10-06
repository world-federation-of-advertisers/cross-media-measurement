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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.expireDataAvailabilitySyncLeasesRequest
import org.wfanet.measurement.internal.edpaggregator.getDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.hasActiveDataAvailabilitySyncLeasesRequest
import org.wfanet.measurement.internal.edpaggregator.releaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.renewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.validateDataAvailabilitySyncLeaseRequest

@RunWith(JUnit4::class)
class SpannerDataAvailabilitySyncLeaseServiceTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `acquire persists active lease`() = runBlocking {
    val service = serviceAt(START_TIME)

    val lease = service.acquireDataAvailabilitySyncLease(acquireRequest())
    val fetched =
      service.getDataAvailabilitySyncLease(
        getDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
        }
      )

    assertThat(lease).isEqualTo(fetched)
    assertThat(lease.state)
      .isEqualTo(DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE)
    assertThat(lease.expireTime.toInstant()).isEqualTo(START_TIME.plus(LEASE_DURATION))
    assertThat(lease.hasCreateTime()).isTrue()
    assertThat(lease.hasUpdateTime()).isTrue()
    assertThat(lease.etag).isNotEmpty()
  }

  @Test
  fun `acquire retry returns existing lease`() = runBlocking {
    val service = serviceAt(START_TIME)
    val request = acquireRequest()

    val first = service.acquireDataAvailabilitySyncLease(request)
    val replay = service.acquireDataAvailabilitySyncLease(request)

    assertThat(replay).isEqualTo(first)
  }

  @Test
  fun `acquire retry rejects an expired lease`() = runBlocking {
    serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plus(LEASE_DURATION))
          .acquireDataAvailabilitySyncLease(acquireRequest())
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `acquire retry rejects an expired lease under healing fence`() = runBlocking {
    serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    insertFence()

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plus(LEASE_DURATION))
          .acquireDataAvailabilitySyncLease(acquireRequest())
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `acquire rejects reused request ID for another attempt`() = runBlocking {
    val service = serviceAt(START_TIME)
    service.acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = OTHER_ATTEMPT_ID
            requestId = ACQUIRE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `acquire rejects another request for an existing attempt`() = runBlocking {
    val service = serviceAt(START_TIME)
    service.acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.acquireDataAvailabilitySyncLease(acquireRequest(OTHER_REQUEST_ID))
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `acquire returns retryable failure while healing fence exists`() = runBlocking {
    insertFence()
    val service = serviceAt(START_TIME)

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.acquireDataAvailabilitySyncLease(acquireRequest())
      }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  @Test
  fun `renew extends an active lease`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    val renewalTime = START_TIME.plus(Duration.ofMinutes(2))

    val renewed =
      serviceAt(renewalTime)
        .renewDataAvailabilitySyncLease(
          renewDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = ATTEMPT_ID
            etag = original.etag
            requestId = RENEW_REQUEST_ID
          }
        )

    assertThat(renewed.expireTime.toInstant()).isEqualTo(renewalTime.plus(LEASE_DURATION))
    assertThat(renewed.etag).isNotEqualTo(original.etag)
  }

  @Test
  fun `renew retry returns existing lease`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    val request = renewDataAvailabilitySyncLeaseRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      synchronizationAttemptId = ATTEMPT_ID
      etag = original.etag
      requestId = RENEW_REQUEST_ID
    }
    val service = serviceAt(START_TIME.plus(Duration.ofMinutes(2)))

    val renewed = service.renewDataAvailabilitySyncLease(request)
    val replay = service.renewDataAvailabilitySyncLease(request)

    assertThat(replay).isEqualTo(renewed)
  }

  @Test
  fun `renew retry rejects an expired lease`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    val renewalTime = START_TIME.plus(Duration.ofMinutes(2))
    val request = renewDataAvailabilitySyncLeaseRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      synchronizationAttemptId = ATTEMPT_ID
      etag = original.etag
      requestId = RENEW_REQUEST_ID
    }
    serviceAt(renewalTime).renewDataAvailabilitySyncLease(request)

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(renewalTime.plus(LEASE_DURATION)).renewDataAvailabilitySyncLease(request)
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `release rejects request ID used to renew`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    val service = serviceAt(START_TIME.plus(Duration.ofMinutes(2)))
    service.renewDataAvailabilitySyncLease(
      renewDataAvailabilitySyncLeaseRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        synchronizationAttemptId = ATTEMPT_ID
        etag = original.etag
        requestId = RENEW_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.releaseDataAvailabilitySyncLease(
          releaseDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = ATTEMPT_ID
            etag = original.etag
            requestId = RENEW_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `renew rejects an expired lease`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plus(LEASE_DURATION))
          .renewDataAvailabilitySyncLease(
            renewDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = ATTEMPT_ID
              etag = original.etag
              requestId = RENEW_REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `renew rejects stale etag`() = runBlocking {
    serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plusSeconds(1))
          .renewDataAvailabilitySyncLease(
            renewDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = ATTEMPT_ID
              etag = "stale"
              requestId = RENEW_REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `renew returns retryable failure after eviction starts`() = runBlocking {
    val original = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    insertFence(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING)

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plusSeconds(1))
          .renewDataAvailabilitySyncLease(
            renewDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = ATTEMPT_ID
              etag = original.etag
              requestId = RENEW_REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  @Test
  fun `validate returns an active lease`() = runBlocking {
    val service = serviceAt(START_TIME)
    val lease = service.acquireDataAvailabilitySyncLease(acquireRequest())

    val validated =
      service.validateDataAvailabilitySyncLease(
        validateDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
          etag = lease.etag
        }
      )

    assertThat(validated).isEqualTo(lease)
  }

  @Test
  fun `validate rejects an expired lease`() = runBlocking {
    val lease = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plus(LEASE_DURATION))
          .validateDataAvailabilitySyncLease(
            validateDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = ATTEMPT_ID
              etag = lease.etag
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `validate returns retryable failure after eviction starts`() = runBlocking {
    val lease = serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    insertFence(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING)

    val error =
      assertFailsWith<StatusRuntimeException> {
        serviceAt(START_TIME.plusSeconds(1))
          .validateDataAvailabilitySyncLease(
            validateDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = ATTEMPT_ID
              etag = lease.etag
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  @Test
  fun `release is idempotent`() = runBlocking {
    val service = serviceAt(START_TIME)
    val original = service.acquireDataAvailabilitySyncLease(acquireRequest())
    val request = releaseDataAvailabilitySyncLeaseRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      synchronizationAttemptId = ATTEMPT_ID
      etag = original.etag
      requestId = RELEASE_REQUEST_ID
    }

    val released = service.releaseDataAvailabilitySyncLease(request)
    val replay = service.releaseDataAvailabilitySyncLease(request)

    assertThat(released.state)
      .isEqualTo(DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED)
    assertThat(replay).isEqualTo(released)
  }

  @Test
  fun `terminal release rejects a changed replay`() = runBlocking {
    val service = serviceAt(START_TIME)
    val original = service.acquireDataAvailabilitySyncLease(acquireRequest())
    val released =
      service.releaseDataAvailabilitySyncLease(
        releaseDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
          etag = original.etag
          requestId = RELEASE_REQUEST_ID
        }
      )
    service.releaseDataAvailabilitySyncLease(
      releaseDataAvailabilitySyncLeaseRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        synchronizationAttemptId = ATTEMPT_ID
        etag = released.etag
        requestId = TERMINAL_RELEASE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.releaseDataAvailabilitySyncLease(
          releaseDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = ATTEMPT_ID
            etag = "changed"
            requestId = TERMINAL_RELEASE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `terminal release reserves request ID across attempts`() = runBlocking {
    val service = serviceAt(START_TIME)
    val original = service.acquireDataAvailabilitySyncLease(acquireRequest())
    service.releaseDataAvailabilitySyncLease(
      releaseDataAvailabilitySyncLeaseRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        synchronizationAttemptId = ATTEMPT_ID
        etag = original.etag
        requestId = TERMINAL_RELEASE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = OTHER_ATTEMPT_ID
            requestId = TERMINAL_RELEASE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `expire marks elapsed active leases`() = runBlocking {
    serviceAt(START_TIME).acquireDataAvailabilitySyncLease(acquireRequest())
    val service = serviceAt(START_TIME.plus(LEASE_DURATION))

    service.expireDataAvailabilitySyncLeases(
      expireDataAvailabilitySyncLeasesRequest { dataProviderResourceId = DATA_PROVIDER_ID }
    )
    val lease =
      service.getDataAvailabilitySyncLease(
        getDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
        }
      )

    assertThat(lease.state)
      .isEqualTo(DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_EXPIRED)
  }

  @Test
  fun `has active excludes released and expired leases`() = runBlocking {
    val service = serviceAt(START_TIME)
    val active = service.acquireDataAvailabilitySyncLease(acquireRequest())
    assertThat(hasActive(service)).isTrue()

    service.releaseDataAvailabilitySyncLease(
      releaseDataAvailabilitySyncLeaseRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        synchronizationAttemptId = ATTEMPT_ID
        etag = active.etag
        requestId = RELEASE_REQUEST_ID
      }
    )
    assertThat(hasActive(service)).isFalse()

    service.acquireDataAvailabilitySyncLease(
      acquireDataAvailabilitySyncLeaseRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        synchronizationAttemptId = OTHER_ATTEMPT_ID
        requestId = OTHER_REQUEST_ID
      }
    )
    assertThat(hasActive(serviceAt(START_TIME.plus(LEASE_DURATION)))).isFalse()
  }

  @Test
  fun `concurrent acquire retries create one lease`() = runBlocking {
    val service = serviceAt(START_TIME)
    val request = acquireRequest()

    val leases = coroutineScope {
      List(2) { async { service.acquireDataAvailabilitySyncLease(request) } }.awaitAll()
    }

    assertThat(leases.distinct()).hasSize(1)
  }

  @Test
  fun `concurrent attempts acquire independent leases`() =
    runBlocking<Unit> {
      val service = serviceAt(START_TIME)

      val leases = coroutineScope {
        listOf(
            acquireRequest(),
            acquireDataAvailabilitySyncLeaseRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              synchronizationAttemptId = OTHER_ATTEMPT_ID
              requestId = OTHER_REQUEST_ID
            },
          )
          .map { request -> async { service.acquireDataAvailabilitySyncLease(request) } }
          .awaitAll()
      }

      assertThat(leases.map { it.synchronizationAttemptId })
        .containsExactly(ATTEMPT_ID, OTHER_ATTEMPT_ID)
    }

  private fun serviceAt(instant: Instant) =
    SpannerDataAvailabilitySyncLeaseService(
      spannerDatabase.databaseClient,
      clock = Clock.fixed(instant, ZoneOffset.UTC),
      leaseDuration = LEASE_DURATION,
    )

  private fun acquireRequest(requestId: String = ACQUIRE_REQUEST_ID) =
    acquireDataAvailabilitySyncLeaseRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      synchronizationAttemptId = ATTEMPT_ID
      this.requestId = requestId
    }

  private suspend fun hasActive(service: SpannerDataAvailabilitySyncLeaseService): Boolean =
    service
      .hasActiveDataAvailabilitySyncLeases(
        hasActiveDataAvailabilitySyncLeasesRequest { dataProviderResourceId = DATA_PROVIDER_ID }
      )
      .hasActiveLeases

  private suspend fun insertFence(
    state: VidLabelingEvictionFenceState =
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
  ) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(EVICTION_OPERATION_ID)
          .set("State")
          .to(state)
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val ATTEMPT_ID = "11111111-1111-4111-8111-111111111111"
    private const val OTHER_ATTEMPT_ID = "22222222-2222-4222-8222-222222222222"
    private const val ACQUIRE_REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val RENEW_REQUEST_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val RELEASE_REQUEST_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    private const val OTHER_REQUEST_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    private const val EVICTION_OPERATION_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    private const val TERMINAL_RELEASE_REQUEST_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    private val START_TIME = Instant.parse("2026-09-30T00:00:00Z")
    private val LEASE_DURATION = Duration.ofMinutes(10)
  }
}
