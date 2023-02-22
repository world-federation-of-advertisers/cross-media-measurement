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

package org.wfanet.measurement.integration.gcloud

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseReader
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.ComputationMutations
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseReader
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken.SpannerContinuationTokensService
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.storage.StorageClient

private typealias ComputationsDb =
  ComputationsDatabaseTransactor<
    ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
  >

class DuchyDependencyProviderRule(duchies: Iterable<String>) :
  ProviderRule<(String) -> InProcessDuchy.DuchyDependencies> {
  override val value: (String) -> InProcessDuchy.DuchyDependencies = this::buildDuchyDependencies

  private val computationsDatabaseRules: Map<String, SpannerEmulatorDatabaseRule> =
    duchies.associateWith { SpannerEmulatorDatabaseRule(Schemata.DUCHY_CHANGELOG_PATH) }

  private fun buildDuchyDependencies(duchyId: String): InProcessDuchy.DuchyDependencies {
    val computationsDatabase =
      computationsDatabaseRules[duchyId]
        ?: error("Missing Computations Spanner database for duchy $duchyId")

    return InProcessDuchy.DuchyDependencies(
      buildComputationsDb(computationsDatabase.databaseClient),
      buildStorageClient(duchyId),
      SpannerContinuationTokensService(computationsDatabase.databaseClient)
    )
  }

  private fun buildComputationsDb(
    computationsDatabaseClient: AsyncDatabaseClient
  ): ComputationsDatabase {
    val readOnlyDb =
      GcpSpannerComputationsDatabaseReader(computationsDatabaseClient, ComputationProtocolStages)
    val computationsDb: ComputationsDb =
      GcpSpannerComputationsDatabaseTransactor(
        databaseClient = computationsDatabaseClient,
        computationMutations =
          ComputationMutations(
            ComputationTypes,
            ComputationProtocolStages,
            ComputationProtocolStageDetails
          )
      )

    return object :
      ComputationsDatabase,
      ComputationsDatabaseReader by readOnlyDb,
      ComputationsDb by computationsDb,
      ComputationProtocolStagesEnumHelper<
        ComputationType, ComputationStage
      > by ComputationProtocolStages {}
  }

  private fun buildStorageClient(duchyId: String): StorageClient {
    return GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-$duchyId")
  }

  override fun apply(base: Statement, description: Description): Statement {
    return chainRulesSequentially(computationsDatabaseRules.values.toList())
      .apply(base, description)
  }
}
