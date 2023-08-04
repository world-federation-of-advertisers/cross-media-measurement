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

import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseReader
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.ComputationMutations
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseReader
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken.SpannerContinuationTokensService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

private typealias ComputationsDb =
  ComputationsDatabaseTransactor<
    ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
    >

object SpannerDuchyDataServices {
  @JvmStatic
  fun create(
    storageClient: StorageClient,
    computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
    duchyName: String,
    databaseClient: AsyncDatabaseClient
  ): DuchyDataServices {

    val computationTypeEnumHelper: ComputationTypeEnumHelper<ComputationType> = ComputationTypes
    val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage> =
      ComputationProtocolStages
    val computationProtocolStageDetailsHelper: ComputationProtocolStageDetailsHelper<
      ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
      > = ComputationProtocolStageDetails

    val computationReader =
      GcpSpannerComputationsDatabaseReader(databaseClient, protocolStagesEnumHelper)
    val computationDb = GcpSpannerComputationsDatabaseTransactor(
      databaseClient = databaseClient,
      computationMutations =
      ComputationMutations(
        computationTypeEnumHelper,
        protocolStagesEnumHelper,
        computationProtocolStageDetailsHelper
      )
    )
    val computationsDatabase = newComputationsDatabase(
      computationReader,
      computationDb,
      protocolStagesEnumHelper
    )
    return DuchyDataServices(
      ComputationsService(
        computationsDatabase = computationsDatabase,
        computationLogEntriesClient = computationLogEntriesClient,
        computationStore = ComputationStore(storageClient),
        requisitionStore = RequisitionStore(storageClient),
        duchyName = duchyName
      ),
      ComputationStatsService(computationsDatabase),
      SpannerContinuationTokensService(databaseClient)
    )
  }

  private fun newComputationsDatabase(
    computationsDatabaseReader: ComputationsDatabaseReader,
    computationDb: ComputationsDb,
    protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>,
  ): ComputationsDatabase {
    return object :
      ComputationsDatabase,
      ComputationsDatabaseReader by computationsDatabaseReader,
      ComputationsDb by computationDb,
      ComputationProtocolStagesEnumHelper<
        ComputationType, ComputationStage
        > by protocolStagesEnumHelper {}
  }
}
