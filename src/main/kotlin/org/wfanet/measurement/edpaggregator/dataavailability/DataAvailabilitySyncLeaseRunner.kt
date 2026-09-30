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

import io.grpc.Status
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.releaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.renewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.validateDataAvailabilitySyncLeaseRequest

/** Client for data-availability synchronization leases. */
interface DataAvailabilitySyncLeaseClient {
  suspend fun acquire(
    parent: String,
    attemptId: String,
    requestId: String,
  ): DataAvailabilitySyncLease

  suspend fun renew(lease: DataAvailabilitySyncLease, requestId: String): DataAvailabilitySyncLease

  suspend fun validate(lease: DataAvailabilitySyncLease): DataAvailabilitySyncLease

  suspend fun release(
    lease: DataAvailabilitySyncLease,
    requestId: String,
  ): DataAvailabilitySyncLease
}

/** gRPC client for data-availability synchronization leases. */
class GrpcDataAvailabilitySyncLeaseClient(
  private val leaseStub: DataAvailabilitySyncLeaseServiceCoroutineStub
) : DataAvailabilitySyncLeaseClient {
  override suspend fun acquire(
    parent: String,
    attemptId: String,
    requestId: String,
  ): DataAvailabilitySyncLease =
    leaseStub.acquireDataAvailabilitySyncLease(
      acquireDataAvailabilitySyncLeaseRequest {
        name = "$parent/dataAvailabilitySyncLeases/$attemptId"
        this.requestId = requestId
      }
    )

  override suspend fun renew(
    lease: DataAvailabilitySyncLease,
    requestId: String,
  ): DataAvailabilitySyncLease =
    leaseStub.renewDataAvailabilitySyncLease(
      renewDataAvailabilitySyncLeaseRequest {
        name = lease.name
        etag = lease.etag
        this.requestId = requestId
      }
    )

  override suspend fun validate(lease: DataAvailabilitySyncLease): DataAvailabilitySyncLease =
    leaseStub.validateDataAvailabilitySyncLease(
      validateDataAvailabilitySyncLeaseRequest {
        name = lease.name
        etag = lease.etag
      }
    )

  override suspend fun release(
    lease: DataAvailabilitySyncLease,
    requestId: String,
  ): DataAvailabilitySyncLease =
    leaseStub.releaseDataAvailabilitySyncLease(
      releaseDataAvailabilitySyncLeaseRequest {
        name = lease.name
        etag = lease.etag
        this.requestId = requestId
      }
    )
}

/** Runs data-availability synchronization while holding a renewable lease. */
class DataAvailabilitySyncLeaseRunner(
  private val leaseClient: DataAvailabilitySyncLeaseClient,
  private val renewalInterval: Duration = DEFAULT_RENEWAL_INTERVAL,
  private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
) {
  init {
    require(renewalInterval.isPositive()) { "renewalInterval must be positive" }
  }

  suspend fun <T> run(
    dataProviderName: String,
    synchronize: suspend (ensureLeaseActive: suspend () -> Unit) -> T,
  ): T {
    val attemptId = uuidGenerator()
    val lease = leaseClient.acquire(dataProviderName, attemptId, uuidGenerator())
    if (lease.state != DataAvailabilitySyncLease.State.ACTIVE) {
      throw Status.FAILED_PRECONDITION.withDescription("Synchronization lease is not active")
        .asRuntimeException()
    }
    val currentLease = AtomicReference(lease)
    val renewalMutex = Mutex()
    suspend fun renewLease() {
      renewalMutex.withLock {
        currentLease.set(leaseClient.renew(currentLease.get(), uuidGenerator()))
      }
    }
    suspend fun ensureLeaseActive() {
      renewalMutex.withLock { currentLease.set(leaseClient.validate(currentLease.get())) }
    }
    var failure: Throwable? = null
    return try {
      coroutineScope {
        val renewalJob = launch {
          while (true) {
            delay(renewalInterval)
            withContext(NonCancellable) { renewLease() }
          }
        }
        try {
          synchronize(::ensureLeaseActive)
        } finally {
          renewalJob.cancelAndJoin()
        }
      }
    } catch (e: Throwable) {
      failure = e
      throw e
    } finally {
      try {
        withContext(NonCancellable) {
          val current = currentLease.get()
          leaseClient.release(current, uuidGenerator())
        }
      } catch (releaseFailure: Throwable) {
        if (failure == null) {
          throw releaseFailure
        }
        failure.addSuppressed(releaseFailure)
      }
    }
  }

  companion object {
    private val DEFAULT_RENEWAL_INTERVAL: Duration = 5.minutes
  }
}
