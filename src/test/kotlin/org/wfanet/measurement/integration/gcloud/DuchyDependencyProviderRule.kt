// Copyright 2020 The Measurement System Authors
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
import java.time.Duration
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.crypto.ElGamalKeyPair
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsRelationalDb
import org.wfanet.measurement.duchy.db.computation.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.SpannerMetricValueDatabase
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.ComputationMutations
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDb
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerReadOnlyComputationsRelationalDb
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.COMPUTATIONS_SCHEMA
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.METRIC_VALUES_SCHEMA
import org.wfanet.measurement.duchy.testing.DUCHY_PUBLIC_KEYS
import org.wfanet.measurement.duchy.testing.DUCHY_SECRET_KEYS
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.common.DUCHY_IDS
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.storage.StorageClient

private typealias ComputationsDb =
  ComputationsRelationalDb<
    ComputationType,
    ComputationStage,
    ComputationStageDetails,
    ComputationDetails
    >

class DuchyDependencyProviderRule(
  duchyIds: Iterable<String>
) : ProviderRule<(Duchy) -> InProcessDuchy.DuchyDependencies> {
  override val value: (Duchy) -> InProcessDuchy.DuchyDependencies = this::buildDuchyDependencies

  private val metricValueDatabaseRules =
    duchyIds
      .map { it to SpannerEmulatorDatabaseRule(METRIC_VALUES_SCHEMA) }
      .toMap()

  private val computationsDatabaseRules =
    duchyIds
      .map { it to SpannerEmulatorDatabaseRule(COMPUTATIONS_SCHEMA) }
      .toMap()

  override fun apply(base: Statement, description: Description): Statement {
    val rules = metricValueDatabaseRules.values + computationsDatabaseRules.values
    return chainRulesSequentially(rules).apply(base, description)
  }

  private fun buildDuchyDependencies(duchy: Duchy): InProcessDuchy.DuchyDependencies {
    val metricValueDatabase = metricValueDatabaseRules[duchy.name]
      ?: error("Missing MetricValue Spanner database for duchy $duchy")
    val computationsDatabase = computationsDatabaseRules[duchy.name]
      ?: error("Missing Computations Spanner database for duchy $duchy")

    return InProcessDuchy.DuchyDependencies(
      buildComputationsDb(duchy.name, computationsDatabase.databaseClient),
      buildMetricValueDb(metricValueDatabase.databaseClient),
      buildStorageClient(duchy.name),
      DUCHY_PUBLIC_KEYS,
      buildCryptoKeySet(duchy.name)
    )
  }

  private fun buildComputationsDb(
    duchyId: String,
    computationsDatabaseClient: AsyncDatabaseClient
  ): ComputationsDatabase {
    val otherDuchyNames = (DUCHY_IDS.toSet() - duchyId).toList()
    val protocolStageEnumHelper = ComputationProtocolStages
    val stageDetails = ComputationProtocolStageDetails(otherDuchyNames)
    val readOnlyDb = GcpSpannerReadOnlyComputationsRelationalDb(
      computationsDatabaseClient,
      protocolStageEnumHelper
    )
    val computationsDb: ComputationsDb =
      GcpSpannerComputationsDb(
        databaseClient = computationsDatabaseClient,
        computationMutations = ComputationMutations(
          ComputationTypes, protocolStageEnumHelper, stageDetails
        ),
        lockDuration = Duration.ofSeconds(1)
      )

    return object :
      ComputationsDatabase,
      ReadOnlyComputationsRelationalDb by readOnlyDb,
      ComputationsDb by computationsDb,
      ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
      by protocolStageEnumHelper {
    }
  }

  private fun buildMetricValueDb(databaseClient: AsyncDatabaseClient): MetricValueDatabase {
    return SpannerMetricValueDatabase(databaseClient, RandomIdGenerator())
  }

  private fun buildStorageClient(duchyId: String): StorageClient {
    return GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-$duchyId")
  }

  private fun buildCryptoKeySet(duchyId: String): CryptoKeySet {
    val latestDuchyPublicKeys = DUCHY_PUBLIC_KEYS.latest
    return CryptoKeySet(
      ownPublicAndPrivateKeys = ElGamalKeyPair.newBuilder().apply {
        publicKey = latestDuchyPublicKeys.getValue(duchyId)
        secretKey = checkNotNull(DUCHY_SECRET_KEYS[duchyId]) { "Secret key not found for $duchyId" }
      }.build(),
      otherDuchyPublicKeys = latestDuchyPublicKeys.mapValues { it.value },
      clientPublicKey = latestDuchyPublicKeys.combinedPublicKey,
      curveId = latestDuchyPublicKeys.curveId
    )
  }
}
