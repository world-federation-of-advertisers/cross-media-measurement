package org.wfanet.measurement.integration

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.db.duchy.computation.ComputationsBlobDb
import org.wfanet.measurement.db.duchy.computation.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.db.duchy.computation.ProtocolStageEnumHelper
import org.wfanet.measurement.db.duchy.computation.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.SingleProtocolDatabase
import org.wfanet.measurement.db.duchy.computation.gcp.ComputationMutations
import org.wfanet.measurement.db.duchy.computation.gcp.GcpSpannerComputationsDb
import org.wfanet.measurement.db.duchy.computation.gcp.GcpSpannerReadOnlyComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.gcp.GcpStorageComputationsDb
import org.wfanet.measurement.db.duchy.metricvalue.MetricValueDatabase
import org.wfanet.measurement.db.duchy.metricvalue.gcp.SpannerMetricValueDatabase
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.duchy.mill.CryptoKeySet
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.gcs.GcsStorageClient

private const val METRIC_VALUE_SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/metric_values.sdl"
private const val COMPUTATIONS_SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/computations.sdl"

class GcpDuchyDependencyProviderRule : ProviderRule<(Duchy) -> InProcessDuchy.DuchyDependencies> {
  override val value: (Duchy) -> InProcessDuchy.DuchyDependencies = this::buildDuchyDependencies

  private val spannerEmulatorDatabaseRules = mutableListOf<SpannerEmulatorDatabaseRule>()

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        val chain = chainRulesSequentially(spannerEmulatorDatabaseRules)
        chain.apply(base, description)
      }
    }
  }

  private fun buildDuchyDependencies(duchy: Duchy): InProcessDuchy.DuchyDependencies {
    val metricValueDatabase = SpannerEmulatorDatabaseRule(METRIC_VALUE_SCHEMA_RESOURCE_PATH)
    spannerEmulatorDatabaseRules.add(metricValueDatabase)

    val computationsDatabase = SpannerEmulatorDatabaseRule(COMPUTATIONS_SCHEMA_RESOURCE_PATH)
    spannerEmulatorDatabaseRules.add(computationsDatabase)

    return InProcessDuchy.DuchyDependencies(
      buildSingleProtocolDb(duchy.name, computationsDatabase.databaseClient),
      buildBlobDb(duchy.name),
      buildMetricValueDb(metricValueDatabase.databaseClient),
      buildStorageClient(duchy.name),
      buildCryptoKeySet(duchy.name)
    )
  }

  private fun buildSingleProtocolDb(
    duchyId: String,
    computationsDatabaseClient: DatabaseClient
  ): SingleProtocolDatabase {
    val otherDuchyNames = (DUCHY_IDS.toSet() - duchyId).toList()
    val stageEnumHelper = LiquidLegionsSketchAggregationProtocol.ComputationStages
    val stageDetails =
      LiquidLegionsSketchAggregationProtocol.ComputationStages.Details(otherDuchyNames)
    val readOnlyDb = GcpSpannerReadOnlyComputationsRelationalDb(
      computationsDatabaseClient,
      stageEnumHelper
    )
    val computationsDb: ComputationsRelationalDb<ComputationStage, ComputationStageDetails> =
      GcpSpannerComputationsDb(
        databaseClient = computationsDatabaseClient,
        duchyName = duchyId,
        duchyOrder = DUCHY_ORDER,
        blobStorageBucket = "mill-computation-stage-storage-$duchyId",
        computationMutations = ComputationMutations(stageEnumHelper, stageDetails)
      )

    return object :
      SingleProtocolDatabase,
      ReadOnlyComputationsRelationalDb by readOnlyDb,
      ComputationsRelationalDb<ComputationStage, ComputationStageDetails> by computationsDb,
      ProtocolStageEnumHelper<ComputationStage> by stageEnumHelper {
      override val computationType =
        ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
    }
  }

  private fun buildBlobDb(
    duchyId: String
  ): ComputationsBlobDb<LiquidLegionsSketchAggregationStage> {
    return GcpStorageComputationsDb(
      LocalStorageHelper.getOptions().service, "bucket-$duchyId"
    )
  }

  private fun buildMetricValueDb(databaseClient: DatabaseClient): MetricValueDatabase {
    return SpannerMetricValueDatabase(databaseClient, RandomIdGenerator())
  }

  private fun buildStorageClient(duchyId: String): StorageClient {
    return GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-$duchyId")
  }

  private fun buildCryptoKeySet(duchyId: String): CryptoKeySet {
    return CryptoKeySet(
      requireNotNull(EL_GAMAL_KEYS[duchyId]),
      EL_GAMAL_KEYS.filter { it.key != duchyId }.map { it.key to it.value.elGamalPk }.toMap(),
      CLIENT_PUBLIC_KEY,
      CURVE_ID
    )
  }
}
