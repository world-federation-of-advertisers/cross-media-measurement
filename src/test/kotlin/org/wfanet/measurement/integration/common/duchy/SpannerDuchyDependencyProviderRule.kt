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

package org.wfanet.measurement.integration.common.duchy

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.deploy.common.service.SpannerDuchyDataServices
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

class SpannerDuchyDependencyProviderRule(duchies: Iterable<String>) :
  ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies> {
  private val computationsDatabaseRules: Map<String, SpannerEmulatorDatabaseRule> =
    duchies.associateWith { SpannerEmulatorDatabaseRule(Schemata.DUCHY_CHANGELOG_PATH) }

  private fun buildDuchyDependencies(
    duchyId: String,
    logEntryClient: ComputationLogEntriesCoroutineStub
  ): InProcessDuchy.DuchyDependencies {
    val computationsDatabase =
      computationsDatabaseRules[duchyId]
        ?: error("Missing Computations Spanner database for duchy $duchyId")
    val storageClient = GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-$duchyId")

    val duchyDataServices =
      SpannerDuchyDataServices.create(
        storageClient,
        logEntryClient,
        duchyId,
        computationsDatabase.databaseClient
      )
    return InProcessDuchy.DuchyDependencies(duchyDataServices, storageClient)
  }

  override val value:
    (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
    get() = ::buildDuchyDependencies

  override fun apply(base: Statement, description: Description): Statement {
    return chainRulesSequentially(computationsDatabaseRules.values.toList())
      .apply(base, description)
  }
}
