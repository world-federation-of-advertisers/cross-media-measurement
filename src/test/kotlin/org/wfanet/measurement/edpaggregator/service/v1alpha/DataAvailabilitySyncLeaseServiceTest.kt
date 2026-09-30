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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.releaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.renewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.validateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.AcquireDataAvailabilitySyncLeaseRequest as InternalAcquireRequest
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase as InternalService
import org.wfanet.measurement.internal.edpaggregator.ReleaseDataAvailabilitySyncLeaseRequest as InternalReleaseRequest
import org.wfanet.measurement.internal.edpaggregator.RenewDataAvailabilitySyncLeaseRequest as InternalRenewRequest
import org.wfanet.measurement.internal.edpaggregator.ValidateDataAvailabilitySyncLeaseRequest as InternalValidateRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncLease as internalLease

@RunWith(JUnit4::class)
class DataAvailabilitySyncLeaseServiceTest {
  private var acquired: InternalAcquireRequest? = null
  private var renewed: InternalRenewRequest? = null
  private var released: InternalReleaseRequest? = null
  private var validated: InternalValidateRequest? = null
  private var acquireError: StatusException? = null

  private val internalService =
    object : InternalService() {
      override suspend fun acquireDataAvailabilitySyncLease(request: InternalAcquireRequest) =
        INTERNAL_LEASE.also {
          acquired = request
          acquireError?.let { error -> throw error }
        }

      override suspend fun renewDataAvailabilitySyncLease(request: InternalRenewRequest) =
        INTERNAL_LEASE.also { renewed = request }

      override suspend fun validateDataAvailabilitySyncLease(request: InternalValidateRequest) =
        INTERNAL_LEASE.also { validated = request }

      override suspend fun releaseDataAvailabilitySyncLease(request: InternalReleaseRequest) =
        INTERNAL_LEASE.also { released = request }
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  @Test
  fun `acquire maps resource identity`() = runBlocking {
    val result =
      service()
        .acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            name = LEASE_NAME
            requestId = REQUEST_ID
          }
        )

    assertThat(acquired!!.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(acquired!!.synchronizationAttemptId).isEqualTo(ATTEMPT_ID)
    assertThat(acquired!!.requestId).isEqualTo(REQUEST_ID)
    assertThat(result.name).isEqualTo(LEASE_NAME)
    assertThat(result.state).isEqualTo(DataAvailabilitySyncLease.State.ACTIVE)
  }

  @Test
  fun `renew forwards concurrency fields`() = runBlocking {
    service()
      .renewDataAvailabilitySyncLease(
        renewDataAvailabilitySyncLeaseRequest {
          name = LEASE_NAME
          etag = "etag"
          requestId = REQUEST_ID
        }
      )

    assertThat(renewed!!.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(renewed!!.synchronizationAttemptId).isEqualTo(ATTEMPT_ID)
    assertThat(renewed!!.etag).isEqualTo("etag")
    assertThat(renewed!!.requestId).isEqualTo(REQUEST_ID)
  }

  @Test
  fun `release forwards concurrency fields`() = runBlocking {
    service()
      .releaseDataAvailabilitySyncLease(
        releaseDataAvailabilitySyncLeaseRequest {
          name = LEASE_NAME
          etag = "etag"
          requestId = REQUEST_ID
        }
      )

    assertThat(released!!.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(released!!.synchronizationAttemptId).isEqualTo(ATTEMPT_ID)
    assertThat(released!!.etag).isEqualTo("etag")
    assertThat(released!!.requestId).isEqualTo(REQUEST_ID)
  }

  @Test
  fun `validate forwards current etag`() = runBlocking {
    service()
      .validateDataAvailabilitySyncLease(
        validateDataAvailabilitySyncLeaseRequest {
          name = LEASE_NAME
          etag = "etag"
        }
      )

    assertThat(validated!!.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(validated!!.synchronizationAttemptId).isEqualTo(ATTEMPT_ID)
    assertThat(validated!!.etag).isEqualTo("etag")
  }

  @Test
  fun `acquire rejects noncanonical UUID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        service()
          .acquireDataAvailabilitySyncLease(
            acquireDataAvailabilitySyncLeaseRequest {
              name = "$DATA_PROVIDER/dataAvailabilitySyncLeases/1-1-4111-8111-1"
              requestId = REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `acquire preserves retryable fence failure`() = runBlocking {
    acquireError = Status.UNAVAILABLE.asException()

    val error =
      assertFailsWith<StatusRuntimeException> {
        service()
          .acquireDataAvailabilitySyncLease(
            acquireDataAvailabilitySyncLeaseRequest {
              name = LEASE_NAME
              requestId = REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  private fun service() =
    DataAvailabilitySyncLeaseService(
      org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseServiceGrpcKt
        .DataAvailabilitySyncLeaseServiceCoroutineStub(grpcTestServerRule.channel)
    )

  companion object {
    private const val DATA_PROVIDER_ID = "data-provider"
    private const val DATA_PROVIDER = "dataProviders/$DATA_PROVIDER_ID"
    private const val ATTEMPT_ID = "11111111-1111-4111-8111-111111111111"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val LEASE_NAME = "$DATA_PROVIDER/dataAvailabilitySyncLeases/$ATTEMPT_ID"
    private val INTERNAL_LEASE = internalLease {
      dataProviderResourceId = DATA_PROVIDER_ID
      synchronizationAttemptId = ATTEMPT_ID
      state =
        org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
          .DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE
      etag = "etag"
    }
  }
}
