// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres.writers

import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter

/**
 * [PostgresWriter] to enqueue a computation by its localComputationId for workers to pickup.
 *
 * @param localId local identifier of the computation.
 * @param editVersion the version of the computation.
 * @param delaySeconds a short delay time. The computation will be available in queue after this
 *   time.
 * @param clock See [Clock].
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#2579): Check expectedOwner before
 *   enqueue.
 */
class EnqueueComputation(
  private val localId: Long,
  private val editVersion: Long,
  private val delaySeconds: Long,
  private val clock: Clock,
) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    checkComputationUnmodified(localId, editVersion)

    val writeTime = clock.instant().truncatedTo(ChronoUnit.MICROS)
    enqueueComputation(localId, writeTime, delaySeconds)
  }
}

/**
 * Enqueue the computation by acquire the computation lock with null ownerId
 *
 * @param localId the local identifier for a computation.
 * @param writeTime the timestamp that this entry is updated.
 * @param delaySeconds the delayed time for a computation to be available in queue for workers to
 *   claim.
 */
suspend fun PostgresWriter.TransactionScope.enqueueComputation(
  localId: Long,
  writeTime: Instant,
  delaySeconds: Long,
) {
  acquireComputationLock(
    localId = localId,
    updateTime = writeTime,
    // Set a lock expiration time to be the current time + a delay with no owner. This will
    // prevent anyone from claiming it until the delay has passed.
    //
    // TODO(@renjiezh): Determine if we even need this delay behavior now that the FIFO queue
    // is based on creation time and not lock expiration time.
    //
    // TODO(@renjiezh): Check to make sure the lock isn't actively held by someone other than
    // the caller.
    ownerId = null,
    lockExpirationTime = writeTime.plusSeconds(delaySeconds),
  )
}
