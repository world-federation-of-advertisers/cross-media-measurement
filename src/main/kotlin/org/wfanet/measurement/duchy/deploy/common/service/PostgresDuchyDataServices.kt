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

package org.wfanet.measurement.duchy.deploy.common.service

import java.time.Clock
import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.common.postgres.PostgresComputationStatsService
import org.wfanet.measurement.duchy.deploy.common.postgres.PostgresComputationsService
import org.wfanet.measurement.duchy.deploy.common.postgres.PostgresContinuationTokensService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

object PostgresDuchyDataServices {
  fun create(
    storageClient: StorageClient,
    computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
    duchyName: String,
    idGenerator: IdGenerator,
    client: DatabaseClient,
    coroutineContext: CoroutineContext,
  ): DuchyDataServices {
    val computationTypeEnumHelper: ComputationTypeEnumHelper<ComputationType> = ComputationTypes
    val protocolStagesEnumHelper:
      ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage> =
      ComputationProtocolStages
    val computationProtocolStageDetailsHelper:
      ComputationProtocolStageDetailsHelper<
        ComputationType,
        ComputationStage,
        ComputationStageDetails,
        ComputationDetails,
      > =
      ComputationProtocolStageDetails

    return DuchyDataServices(
      PostgresComputationsService(
        computationTypeEnumHelper = computationTypeEnumHelper,
        protocolStagesEnumHelper = protocolStagesEnumHelper,
        computationProtocolStageDetailsHelper = computationProtocolStageDetailsHelper,
        client = client,
        idGenerator = idGenerator,
        duchyName = duchyName,
        computationStore = ComputationStore(storageClient),
        requisitionStore = RequisitionStore(storageClient),
        computationLogEntriesClient = computationLogEntriesClient,
        coroutineContext = coroutineContext,
        clock = Clock.systemUTC(),
      ),
      PostgresComputationStatsService(client, idGenerator),
      PostgresContinuationTokensService(client, idGenerator),
    )
  }
}
