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

import com.google.cloud.spanner.Options
import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.DataAvailabilitySyncLeaseResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.expireDataAvailabilitySyncLeases
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findDataAvailabilitySyncLeaseByMutationRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.hasActiveDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AcquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLease
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
import org.wfanet.measurement.internal.edpaggregator.ExpireDataAvailabilitySyncLeasesRequest
import org.wfanet.measurement.internal.edpaggregator.GetDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.HasActiveDataAvailabilitySyncLeasesRequest
import org.wfanet.measurement.internal.edpaggregator.HasActiveDataAvailabilitySyncLeasesResponse
import org.wfanet.measurement.internal.edpaggregator.ReleaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.RenewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.ValidateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.hasActiveDataAvailabilitySyncLeasesResponse

/** Spanner-backed data-availability synchronization leases. */
class SpannerDataAvailabilitySyncLeaseService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val clock: Clock = Clock.systemUTC(),
  private val leaseDuration: Duration = DEFAULT_LEASE_DURATION,
) : DataAvailabilitySyncLeaseServiceCoroutineImplBase(coroutineContext) {
  init {
    require(!leaseDuration.isNegative && !leaseDuration.isZero) { "leaseDuration must be positive" }
  }

  override suspend fun acquireDataAvailabilitySyncLease(
    request: AcquireDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    validateIdentity(request.dataProviderResourceId, request.synchronizationAttemptId)
    validateUuid(request.requestId, "request_id")
    val requestFingerprint = request.fingerprint()
    val now = clock.instant()
    val expireTime = now.plus(leaseDuration)
    val result =
      databaseClient
        .readWriteTransaction(Options.tag("action=acquireDataAvailabilitySyncLease"))
        .run { txn ->
          findReplay(
              txn,
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
              request.requestId,
              requestFingerprint,
            )
            ?.let {
              requireActive(it, now)
              return@run MutationResult(it)
            }
          if (
            txn.findDataAvailabilitySyncLease(
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
            ) != null
          ) {
            throw Status.ALREADY_EXISTS.withDescription(
                "DataAvailabilitySyncLease ${request.synchronizationAttemptId} already exists"
              )
              .asRuntimeException()
          }
          if (txn.getVidLabelingEvictionFence(request.dataProviderResourceId) != null) {
            throw Status.UNAVAILABLE.withDescription(
                "Data availability synchronization is fenced for DataProvider " +
                  request.dataProviderResourceId
              )
              .asRuntimeException()
          }
          txn.insertDataAvailabilitySyncLease(
            request.dataProviderResourceId,
            request.synchronizationAttemptId,
            expireTime,
            request.requestId,
            requestFingerprint,
          )
          MutationResult()
        }
    return result.replay
      ?: getLease(request.dataProviderResourceId, request.synchronizationAttemptId)
  }

  override suspend fun getDataAvailabilitySyncLease(
    request: GetDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    validateIdentity(request.dataProviderResourceId, request.synchronizationAttemptId)
    return getLease(request.dataProviderResourceId, request.synchronizationAttemptId)
  }

  override suspend fun validateDataAvailabilitySyncLease(
    request: ValidateDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    validateIdentity(request.dataProviderResourceId, request.synchronizationAttemptId)
    requireField(request.etag, "etag")
    return databaseClient
      .readWriteTransaction(Options.tag("action=validateDataAvailabilitySyncLease"))
      .run { txn ->
        requireRenewalPermitted(txn, request.dataProviderResourceId)
        val result =
          txn.findDataAvailabilitySyncLease(
            request.dataProviderResourceId,
            request.synchronizationAttemptId,
          ) ?: throw notFound(request.synchronizationAttemptId)
        requireActive(result, clock.instant())
        checkEtag(request.etag, result.dataAvailabilitySyncLease.etag)
        result.dataAvailabilitySyncLease
      }
  }

  override suspend fun renewDataAvailabilitySyncLease(
    request: RenewDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    validateIdentity(request.dataProviderResourceId, request.synchronizationAttemptId)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    val requestFingerprint = request.fingerprint()
    val now = clock.instant()
    val expireTime = now.plus(leaseDuration)
    val mutationResult =
      databaseClient
        .readWriteTransaction(Options.tag("action=renewDataAvailabilitySyncLease"))
        .run { txn ->
          requireRenewalPermitted(txn, request.dataProviderResourceId)
          findReplay(
              txn,
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
              request.requestId,
              requestFingerprint,
            )
            ?.let {
              requireActive(it, now)
              return@run MutationResult(it)
            }
          val result =
            txn.findDataAvailabilitySyncLease(
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
            ) ?: throw notFound(request.synchronizationAttemptId)
          requireActive(result, now)
          checkEtag(request.etag, result.dataAvailabilitySyncLease.etag)
          txn.updateDataAvailabilitySyncLease(
            result,
            DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE,
            expireTime,
            request.requestId,
            requestFingerprint,
          )
          MutationResult()
        }
    return mutationResult.replay
      ?: getLease(request.dataProviderResourceId, request.synchronizationAttemptId)
  }

  override suspend fun releaseDataAvailabilitySyncLease(
    request: ReleaseDataAvailabilitySyncLeaseRequest
  ): DataAvailabilitySyncLease {
    validateIdentity(request.dataProviderResourceId, request.synchronizationAttemptId)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    val requestFingerprint = request.fingerprint()
    val now = clock.instant()
    val mutationResult =
      databaseClient
        .readWriteTransaction(Options.tag("action=releaseDataAvailabilitySyncLease"))
        .run { txn ->
          findReplay(
              txn,
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
              request.requestId,
              requestFingerprint,
            )
            ?.let {
              return@run MutationResult(it)
            }
          val result =
            txn.findDataAvailabilitySyncLease(
              request.dataProviderResourceId,
              request.synchronizationAttemptId,
            ) ?: throw notFound(request.synchronizationAttemptId)
          if (
            result.dataAvailabilitySyncLease.state ==
              DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED ||
              result.dataAvailabilitySyncLease.state ==
                DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_EXPIRED
          ) {
            txn.updateDataAvailabilitySyncLease(
              result,
              result.dataAvailabilitySyncLease.state,
              result.dataAvailabilitySyncLease.expireTime.toInstant(),
              request.requestId,
              requestFingerprint,
            )
            return@run MutationResult()
          }
          checkEtag(request.etag, result.dataAvailabilitySyncLease.etag)
          txn.updateDataAvailabilitySyncLease(
            result,
            DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED,
            result.dataAvailabilitySyncLease.expireTime.toInstant(),
            request.requestId,
            requestFingerprint,
          )
          MutationResult()
        }
    return mutationResult.replay
      ?: getLease(request.dataProviderResourceId, request.synchronizationAttemptId)
  }

  override suspend fun expireDataAvailabilitySyncLeases(
    request: ExpireDataAvailabilitySyncLeasesRequest
  ): Empty {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    databaseClient
      .readWriteTransaction(Options.tag("action=expireDataAvailabilitySyncLeases"))
      .run { txn ->
        txn.expireDataAvailabilitySyncLeases(request.dataProviderResourceId, clock.instant())
      }
    return Empty.getDefaultInstance()
  }

  override suspend fun hasActiveDataAvailabilitySyncLeases(
    request: HasActiveDataAvailabilitySyncLeasesRequest
  ): HasActiveDataAvailabilitySyncLeasesResponse {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    val hasActiveLeases =
      databaseClient.singleUse().use { txn ->
        txn.hasActiveDataAvailabilitySyncLease(request.dataProviderResourceId, clock.instant())
      }
    return hasActiveDataAvailabilitySyncLeasesResponse { this.hasActiveLeases = hasActiveLeases }
  }

  private suspend fun getLease(
    dataProviderResourceId: String,
    synchronizationAttemptId: String,
  ): DataAvailabilitySyncLease =
    databaseClient.singleUse().use { txn ->
      txn
        .findDataAvailabilitySyncLease(dataProviderResourceId, synchronizationAttemptId)
        ?.dataAvailabilitySyncLease ?: throw notFound(synchronizationAttemptId)
    }

  private suspend fun findReplay(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
    synchronizationAttemptId: String,
    requestId: String,
    requestFingerprint: ByteString,
  ): DataAvailabilitySyncLease? {
    val result =
      txn.findDataAvailabilitySyncLeaseByMutationRequestId(dataProviderResourceId, requestId)
        ?: return null
    val requestIndex = result.mutationRequestIds.indexOf(requestId)
    if (
      result.dataAvailabilitySyncLease.synchronizationAttemptId != synchronizationAttemptId ||
        result.mutationRequestFingerprints[requestIndex] != requestFingerprint
    ) {
      throw Status.ALREADY_EXISTS.withDescription("request_id has already been used")
        .asRuntimeException()
    }
    return result.dataAvailabilitySyncLease
  }

  private suspend fun requireRenewalPermitted(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
  ) {
    if (
      txn.getVidLabelingEvictionFence(dataProviderResourceId)?.state ==
        VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
    ) {
      throw Status.UNAVAILABLE.withDescription(
          "Data availability synchronization is fenced for DataProvider $dataProviderResourceId"
        )
        .asRuntimeException()
    }
  }

  private fun requireActive(result: DataAvailabilitySyncLeaseResult, now: java.time.Instant) {
    requireActive(result.dataAvailabilitySyncLease, now)
  }

  private fun requireActive(lease: DataAvailabilitySyncLease, now: java.time.Instant) {
    if (
      lease.state != DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE ||
        !lease.expireTime.toInstant().isAfter(now)
    ) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "DataAvailabilitySyncLease ${lease.synchronizationAttemptId} is not active"
        )
        .asRuntimeException()
    }
  }

  private fun validateIdentity(dataProviderResourceId: String, synchronizationAttemptId: String) {
    requireField(dataProviderResourceId, "data_provider_resource_id")
    validateUuid(synchronizationAttemptId, "synchronization_attempt_id")
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
        throw IllegalArgumentException("$field must be a UUID4")
      }
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(field, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun checkEtag(requestEtag: String, etag: String) {
    try {
      EtagMismatchException.check(requestEtag, etag)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  private fun notFound(synchronizationAttemptId: String) =
    Status.NOT_FOUND.withDescription(
        "DataAvailabilitySyncLease $synchronizationAttemptId not found"
      )
      .asRuntimeException()

  private fun AcquireDataAvailabilitySyncLeaseRequest.fingerprint(): ByteString = fingerprint(this)

  private fun RenewDataAvailabilitySyncLeaseRequest.fingerprint(): ByteString = fingerprint(this)

  private fun ReleaseDataAvailabilitySyncLeaseRequest.fingerprint(): ByteString = fingerprint(this)

  private fun fingerprint(message: com.google.protobuf.Message): ByteString =
    MessageDigest.getInstance("SHA-256")
      .apply {
        update(message.descriptorForType.fullName.toByteArray(Charsets.UTF_8))
        update(byteArrayOf(0))
      }
      .digest(message.toByteArray())
      .toByteString()

  private data class MutationResult(val replay: DataAvailabilitySyncLease? = null)

  companion object {
    private val DEFAULT_LEASE_DURATION: Duration = Duration.ofMinutes(10)
  }
}
