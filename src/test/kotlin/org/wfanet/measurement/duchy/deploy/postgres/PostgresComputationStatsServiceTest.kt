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

package org.wfanet.measurement.duchy.deploy.postgres

import java.time.Clock
import kotlin.random.Random
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.common.postgres.PostgresComputationStatsService
import org.wfanet.measurement.duchy.deploy.common.postgres.PostgresComputationsService
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH
import org.wfanet.measurement.duchy.service.internal.testing.ComputationStatsServiceTest
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

private const val ALSACE = "Alsace"

@RunWith(JUnit4::class)
class PostgresComputationStatsServiceTest :
  ComputationStatsServiceTest<PostgresComputationStatsService>() {

  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore
  private val tempDirectory = TemporaryFolder()
  private val mockComputationLogEntriesService: ComputationLogEntriesCoroutineImplBase =
    mockService()

  private val client = EmbeddedPostgresDatabaseProvider(DUCHY_CHANGELOG_PATH).createNewDatabase()
  private val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  private val grpcTestServerRule = GrpcTestServerRule {
    storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    addService(mockComputationLogEntriesService)
  }
  private val systemComputationLogEntriesClient =
    ComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)

  override fun newComputationStatsService(): PostgresComputationStatsService {
    return PostgresComputationStatsService(client, idGenerator)
  }

  override fun newComputationsService(): ComputationsGrpcKt.ComputationsCoroutineImplBase {
    return PostgresComputationsService(
      computationTypeEnumHelper = ComputationTypes,
      protocolStagesEnumHelper = ComputationProtocolStages,
      computationProtocolStageDetailsHelper = ComputationProtocolStageDetails,
      client = client,
      idGenerator = idGenerator,
      duchyName = ALSACE,
      computationLogEntriesClient = systemComputationLogEntriesClient
    )
  }
}
