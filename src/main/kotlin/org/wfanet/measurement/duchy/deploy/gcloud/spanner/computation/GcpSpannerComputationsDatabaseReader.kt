// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import java.time.Instant
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toCollection
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages.stageToProtocol
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseReader
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey

/** Implementation of [ComputationsDatabaseReader] using GCP Spanner Database. */
class GcpSpannerComputationsDatabaseReader(
  private val databaseClient: AsyncDatabaseClient,
  private val computationProtocolStagesHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
) : ComputationsDatabaseReader {

  override suspend fun readComputationToken(globalId: String): ComputationToken? {
    return ComputationTokenProtoQuery(
        parseStageEnum = computationProtocolStagesHelper::longValuesToComputationStageEnum,
        globalId = globalId
      )
      .execute(databaseClient)
      .singleOrNull()
  }

  override suspend fun readComputationToken(
    externalRequisitionKey: ExternalRequisitionKey
  ): ComputationToken? {
    return ComputationTokenProtoQuery(
        parseStageEnum = computationProtocolStagesHelper::longValuesToComputationStageEnum,
        externalRequisitionKey = externalRequisitionKey
      )
      .execute(databaseClient)
      .singleOrNull()
  }

  override suspend fun readGlobalComputationIds(
    stages: Set<ComputationStage>,
    updatedBefore: Instant?
  ): Set<String> {
    val computationTypes = stages.map { stageToProtocol(it) }.distinct()
    grpcRequire(computationTypes.count() == 1) {
      "All stages should have the same ComputationType."
    }

    return GlobalIdsQuery(
        ComputationProtocolStages::computationStageEnumToLongValues,
        stages,
        computationTypes[0],
        updatedBefore?.toGcloudTimestamp()
      )
      .execute(databaseClient)
      .toCollection(mutableSetOf())
  }

  override suspend fun readComputationBlobKeys(localId: Long): List<String> {
    return ComputationBlobKeysQuery(localId).execute(databaseClient).toCollection(mutableListOf())
  }

  override suspend fun readRequisitionBlobKeys(localId: Long): List<String> {
    return RequisitionBlobKeysQuery(localId).execute(databaseClient).toCollection(mutableListOf())
  }
}
