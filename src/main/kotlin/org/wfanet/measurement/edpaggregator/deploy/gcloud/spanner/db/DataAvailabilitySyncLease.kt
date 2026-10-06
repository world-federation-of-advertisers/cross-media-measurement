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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLease
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncLease

data class DataAvailabilitySyncLeaseResult(
  val dataAvailabilitySyncLease: DataAvailabilitySyncLease,
  val mutationRequestIds: List<String>,
  val mutationRequestFingerprints: List<ByteString>,
)

/** Reads a data-availability synchronization lease. */
suspend fun AsyncDatabaseClient.ReadContext.findDataAvailabilitySyncLease(
  dataProviderResourceId: String,
  synchronizationAttemptId: String,
): DataAvailabilitySyncLeaseResult? {
  return readRow(
      "DataAvailabilitySyncLease",
      Key.of(dataProviderResourceId, synchronizationAttemptId),
      COLUMNS,
    )
    ?.let(::buildDataAvailabilitySyncLeaseResult)
}

/** Requires an active synchronization lease while reading the current eviction fence. */
suspend fun AsyncDatabaseClient.ReadContext.requireActiveDataAvailabilitySyncLease(
  dataProviderResourceId: String,
  synchronizationAttemptId: String,
  now: Instant,
) {
  if (
    getVidLabelingEvictionFence(dataProviderResourceId)?.state ==
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
  ) {
    throw Status.UNAVAILABLE.withDescription(
        "Data availability synchronization is fenced for DataProvider $dataProviderResourceId"
      )
      .asRuntimeException()
  }
  if (synchronizationAttemptId.isEmpty()) return
  val lease =
    findDataAvailabilitySyncLease(dataProviderResourceId, synchronizationAttemptId)
      ?.dataAvailabilitySyncLease
      ?: throw Status.FAILED_PRECONDITION.withDescription(
          "DataAvailabilitySyncLease $synchronizationAttemptId does not exist"
        )
        .asRuntimeException()
  if (
    lease.state != DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE ||
      !lease.expireTime.toInstant().isAfter(now)
  ) {
    throw Status.FAILED_PRECONDITION.withDescription(
        "DataAvailabilitySyncLease $synchronizationAttemptId is not active"
      )
      .asRuntimeException()
  }
}

/** Finds a data-availability synchronization lease by mutation request ID. */
suspend fun AsyncDatabaseClient.ReadContext.findDataAvailabilitySyncLeaseByMutationRequestId(
  dataProviderResourceId: String,
  requestId: String,
): DataAvailabilitySyncLeaseResult? {
  val sql =
    """
    SELECT ${COLUMNS.joinToString()}
    FROM DataAvailabilitySyncLease
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND @requestId IN UNNEST(MutationRequestIds)
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("requestId").to(requestId)
      },
      Options.tag("action=findDataAvailabilitySyncLeaseByMutationRequestId"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildDataAvailabilitySyncLeaseResult)
}

/** Returns whether a data provider has any unexpired active synchronization lease. */
suspend fun AsyncDatabaseClient.ReadContext.hasActiveDataAvailabilitySyncLease(
  dataProviderResourceId: String,
  now: Instant,
): Boolean {
  val sql =
    """
    SELECT SynchronizationAttemptId
    FROM DataAvailabilitySyncLease@{FORCE_INDEX=DataAvailabilitySyncLeaseByState}
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND State = @activeState
      AND ExpireTime > @now
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("activeState")
          .to(
            Value.protoEnum(
              DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE
            )
          )
        bind("now").to(now.toGcloudTimestamp())
      },
      Options.tag("action=hasActiveDataAvailabilitySyncLease"),
    )
    .singleOrNullIfEmpty() != null
}

/** Returns whether a specific synchronization-attempt lease is active and unexpired. */
suspend fun AsyncDatabaseClient.ReadContext.isDataAvailabilitySyncLeaseActive(
  dataProviderResourceId: String,
  synchronizationAttemptId: String,
  now: Instant,
): Boolean {
  val lease =
    findDataAvailabilitySyncLease(dataProviderResourceId, synchronizationAttemptId)
      ?.dataAvailabilitySyncLease ?: return false
  return lease.state == DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE &&
    lease.expireTime.toInstant().isAfter(now)
}

/** Reads active synchronization leases whose expiration has elapsed. */
fun AsyncDatabaseClient.ReadContext.readExpiredDataAvailabilitySyncLeases(
  dataProviderResourceId: String,
  now: Instant,
): Flow<DataAvailabilitySyncLeaseResult> {
  val sql =
    """
    SELECT ${COLUMNS.joinToString()}
    FROM DataAvailabilitySyncLease@{FORCE_INDEX=DataAvailabilitySyncLeaseByState}
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND State = @activeState
      AND ExpireTime <= @now
    ORDER BY ExpireTime, SynchronizationAttemptId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("activeState")
          .to(
            Value.protoEnum(
              DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE
            )
          )
        bind("now").to(now.toGcloudTimestamp())
      },
      Options.tag("action=readExpiredDataAvailabilitySyncLeases"),
    )
    .map(::buildDataAvailabilitySyncLeaseResult)
}

/** Buffers creation of a data-availability synchronization lease. */
fun AsyncDatabaseClient.TransactionContext.insertDataAvailabilitySyncLease(
  dataProviderResourceId: String,
  synchronizationAttemptId: String,
  expireTime: Instant,
  requestId: String,
  requestFingerprint: ByteString,
) {
  bufferInsertMutation("DataAvailabilitySyncLease") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("SynchronizationAttemptId").to(synchronizationAttemptId)
    set("State").to(DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE)
    set("ExpireTime").to(expireTime.toGcloudTimestamp())
    set("MutationRequestIds").toStringArray(listOf(requestId))
    set("MutationRequestFingerprints").toBytesArray(listOf(requestFingerprint.toGcloudByteArray()))
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a synchronization lease mutation and its idempotency record. */
fun AsyncDatabaseClient.TransactionContext.updateDataAvailabilitySyncLease(
  result: DataAvailabilitySyncLeaseResult,
  state: DataAvailabilitySyncLeaseState,
  expireTime: Instant,
  requestId: String? = null,
  requestFingerprint: ByteString? = null,
) {
  require((requestId == null) == (requestFingerprint == null))
  bufferUpdateMutation("DataAvailabilitySyncLease") {
    set("DataProviderResourceId").to(result.dataAvailabilitySyncLease.dataProviderResourceId)
    set("SynchronizationAttemptId").to(result.dataAvailabilitySyncLease.synchronizationAttemptId)
    set("State").to(state)
    set("ExpireTime").to(expireTime.toGcloudTimestamp())
    if (requestId != null && requestFingerprint != null) {
      set("MutationRequestIds").toStringArray(result.mutationRequestIds + requestId)
      set("MutationRequestFingerprints")
        .toBytesArray(
          buildList<ByteString> {
              addAll(result.mutationRequestFingerprints)
              add(requestFingerprint)
            }
            .map { it.toGcloudByteArray() }
        )
    }
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Marks every elapsed active synchronization lease as expired. */
suspend fun AsyncDatabaseClient.TransactionContext.expireDataAvailabilitySyncLeases(
  dataProviderResourceId: String,
  now: Instant,
) {
  readExpiredDataAvailabilitySyncLeases(dataProviderResourceId, now).collect { result ->
    updateDataAvailabilitySyncLease(
      result,
      DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_EXPIRED,
      result.dataAvailabilitySyncLease.expireTime.toInstant(),
    )
  }
}

private fun buildDataAvailabilitySyncLeaseResult(row: Struct): DataAvailabilitySyncLeaseResult {
  val updateTime = row.getTimestamp("UpdateTime").toProto()
  return DataAvailabilitySyncLeaseResult(
    dataAvailabilitySyncLease {
      dataProviderResourceId = row.getString("DataProviderResourceId")
      synchronizationAttemptId = row.getString("SynchronizationAttemptId")
      state = row.getProtoEnum("State", DataAvailabilitySyncLeaseState::forNumber)
      expireTime = row.getTimestamp("ExpireTime").toProto()
      createTime = row.getTimestamp("CreateTime").toProto()
      this.updateTime = updateTime
      etag = ETags.computeETag(updateTime.toInstant())
    },
    mutationRequestIds = row.getStringList("MutationRequestIds"),
    mutationRequestFingerprints =
      row.getBytesList("MutationRequestFingerprints").map { it.toByteArray().toByteString() },
  )
}

private val COLUMNS =
  listOf(
    "DataProviderResourceId",
    "SynchronizationAttemptId",
    "State",
    "ExpireTime",
    "MutationRequestIds",
    "MutationRequestFingerprints",
    "CreateTime",
    "UpdateTime",
  )
