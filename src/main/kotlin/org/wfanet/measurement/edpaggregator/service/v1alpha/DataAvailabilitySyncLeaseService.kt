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

import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.edpaggregator.service.DataAvailabilitySyncLeaseKey
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.v1alpha.AcquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ReleaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RenewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ValidateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLease as InternalLease
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub as InternalLeaseStub
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState as InternalLeaseState
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest as internalAcquireLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.releaseDataAvailabilitySyncLeaseRequest as internalReleaseLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.renewDataAvailabilitySyncLeaseRequest as internalRenewLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.validateDataAvailabilitySyncLeaseRequest as internalValidateLeaseRequest

/** Public API adapter for data-availability synchronization leases. */
class DataAvailabilitySyncLeaseService(
  private val internalLeaseStub: InternalLeaseStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : DataAvailabilitySyncLeaseServiceCoroutineImplBase(coroutineContext) {

  override suspend fun acquireDataAvailabilitySyncLease(
    request: AcquireDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    val key = parseLease(request.name)
    validateUuid(request.requestId, "request_id")
    return callInternal {
        internalLeaseStub.acquireDataAvailabilitySyncLease(
          internalAcquireLeaseRequest {
            dataProviderResourceId = key.dataProviderId
            synchronizationAttemptId = key.dataAvailabilitySyncLeaseId
            requestId = request.requestId
          }
        )
      }
      .toPublic()
  }

  override suspend fun renewDataAvailabilitySyncLease(
    request: RenewDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    val key = parseLease(request.name)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    return callInternal {
        internalLeaseStub.renewDataAvailabilitySyncLease(
          internalRenewLeaseRequest {
            dataProviderResourceId = key.dataProviderId
            synchronizationAttemptId = key.dataAvailabilitySyncLeaseId
            etag = request.etag
            requestId = request.requestId
          }
        )
      }
      .toPublic()
  }

  override suspend fun validateDataAvailabilitySyncLease(
    request: ValidateDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    val key = parseLease(request.name)
    requireField(request.etag, "etag")
    return callInternal {
        internalLeaseStub.validateDataAvailabilitySyncLease(
          internalValidateLeaseRequest {
            dataProviderResourceId = key.dataProviderId
            synchronizationAttemptId = key.dataAvailabilitySyncLeaseId
            etag = request.etag
          }
        )
      }
      .toPublic()
  }

  override suspend fun releaseDataAvailabilitySyncLease(
    request: ReleaseDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    val key = parseLease(request.name)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    return callInternal {
        internalLeaseStub.releaseDataAvailabilitySyncLease(
          internalReleaseLeaseRequest {
            dataProviderResourceId = key.dataProviderId
            synchronizationAttemptId = key.dataAvailabilitySyncLeaseId
            etag = request.etag
            requestId = request.requestId
          }
        )
      }
      .toPublic()
  }

  private fun parseLease(name: String): DataAvailabilitySyncLeaseKey {
    requireField(name, "name")
    val key = DataAvailabilitySyncLeaseKey.fromName(name) ?: invalid("name")
    validateUuid(key.dataAvailabilitySyncLeaseId, "name")
    return key
  }

  private suspend fun <T> callInternal(block: suspend () -> T): T {
    return try {
      block()
    } catch (e: StatusException) {
      throw e.status.withCause(e).asRuntimeException()
    }
  }

  private fun InternalLease.toPublic(): DataAvailabilitySyncLease = dataAvailabilitySyncLease {
    name = DataAvailabilitySyncLeaseKey(dataProviderResourceId, synchronizationAttemptId).toName()
    state = this@toPublic.state.toPublic()
    expireTime = this@toPublic.expireTime
    createTime = this@toPublic.createTime
    updateTime = this@toPublic.updateTime
    etag = this@toPublic.etag
  }

  private fun InternalLeaseState.toPublic(): DataAvailabilitySyncLease.State =
    when (this) {
      InternalLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE ->
        DataAvailabilitySyncLease.State.ACTIVE
      InternalLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED ->
        DataAvailabilitySyncLease.State.RELEASED
      InternalLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_EXPIRED ->
        DataAvailabilitySyncLease.State.EXPIRED
      InternalLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_UNSPECIFIED,
      InternalLeaseState.UNRECOGNIZED -> DataAvailabilitySyncLease.State.STATE_UNSPECIFIED
    }

  private fun requireField(value: String, field: String) {
    if (value.isEmpty()) {
      throw RequiredFieldNotSetException(field)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun validateUuid(value: String, field: String) {
    requireField(value, field)
    try {
      val uuid = UUID.fromString(value)
      if (
        uuid.version() != 4 ||
          uuid.variant() != 2 ||
          !uuid.toString().equals(value, ignoreCase = true)
      ) {
        invalid(field)
      }
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(field, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun invalid(field: String): Nothing =
    throw InvalidFieldValueException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
}
