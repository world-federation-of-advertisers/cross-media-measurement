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

package org.wfanet.measurement.duchy.deploy.postgres.writers

import java.time.Clock
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter

class EnqueueComputation(
  private val clock: Clock,
  private val localId: Long,
  private val editVersion: Long,
  private val delaySeconds: Long,
) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    checkComputationUnmodified(localId, editVersion)

    val writeTime = clock.instant()
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
      lockExpirationTime = writeTime.plusSeconds(delaySeconds)
    )
  }
}
