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
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationBlobReferenceReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationToken

/**
 * [PostgresWriter] to record the path for a new output blob.
 *
 * @param token the [ComputationEditToken] of the target computation.
 * @param blobRef See [BlobRef].
 * @param clock See [Clock].
 * @param protocolStagesEnumHelper See [ComputationProtocolStagesEnumHelper].
 */
class RecordOutputBlobPath<ProtocolT, StageT>(
  private val token: ComputationEditToken<ProtocolT, StageT>,
  private val blobRef: BlobRef,
  private val clock: Clock,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {
  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    require(blobRef.key.isNotBlank()) { "Cannot insert blank path to blob. $blobRef" }

    val localId = token.localId
    val stage = token.stage

    checkComputationUnmodified(localId, token.editVersion)

    val stageLongValue =
      protocolStagesEnumHelper.computationStageEnumToLongValues(token.stage).stage
    val type: ComputationBlobDependency =
      ComputationBlobReferenceReader()
        .readBlobDependency(
          transactionContext,
          localId,
          stageLongValue,
          blobRef.idInRelationalDatabase,
        )
        ?: error(
          "No ComputationBlobReferences row for " +
            "($localId, $stage, ${blobRef.idInRelationalDatabase})"
        )
    check(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }

    updateComputation(
      localId = localId,
      updateTime = clock.instant().truncatedTo(ChronoUnit.MICROS),
    )

    updateComputationBlobReference(
      localId = localId,
      stage = stageLongValue,
      blobId = blobRef.idInRelationalDatabase,
      pathToBlob = blobRef.key,
    )

    return checkNotNull(computationReader.readComputationToken(transactionContext, token.globalId))
  }
}
