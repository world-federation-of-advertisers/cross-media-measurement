// Copyright 2021 The Cross-Media Measurement Authors
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

import java.time.Clock
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseReader
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.service.internal.testing.ComputationStatsServiceTest
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

private const val ALSACE = "Alsace"

@RunWith(JUnit4::class)
class SpannerComputationStatsServiceTest : ComputationStatsServiceTest<ComputationStatsService>() {
  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.DUCHY_CHANGELOG_PATH)

  private val mockComputationLogEntriesService: ComputationLogEntriesCoroutineImplBase =
    mockService()
  private val tempDirectory = TemporaryFolder()
  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore
  private lateinit var computationsDatabase: ComputationsDatabase

  private val protocolStageEnumHelper = ComputationProtocolStages
  private val computationProtocolStageDetails = ComputationProtocolStageDetails
  val grpcTestServerRule = GrpcTestServerRule {
    storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    addService(mockComputationLogEntriesService)
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private fun initComputationsDatabase() {
    val computationsDatabaseReader =
      GcpSpannerComputationsDatabaseReader(spannerDatabase.databaseClient, protocolStageEnumHelper)
    val computationsDatabaseTransactor =
      GcpSpannerComputationsDatabaseTransactor(
        databaseClient = spannerDatabase.databaseClient,
        computationMutations =
          ComputationMutations(
            ComputationTypes,
            protocolStageEnumHelper,
            computationProtocolStageDetails
          )
      )

    computationsDatabase =
      object :
        ComputationsDatabase,
        ComputationsDatabaseReader by computationsDatabaseReader,
        ComputationsDatabaseTransactor<
          ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
        > by computationsDatabaseTransactor,
        ComputationProtocolStagesEnumHelper<
          ComputationType, ComputationStage
        > by protocolStageEnumHelper {}
  }

  override fun newComputationStatsService(): ComputationStatsService {
    initComputationsDatabase()
    return ComputationStatsService(computationsDatabase)
  }

  override fun newComputationsService(): ComputationsCoroutineImplBase {
    initComputationsDatabase()
    val systemComputationLogEntriesClient =
      ComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)
    return ComputationsService(
      computationsDatabase,
      systemComputationLogEntriesClient,
      ComputationStore(storageClient),
      RequisitionStore(storageClient),
      ALSACE,
      Clock.systemUTC()
    )
  }
}
