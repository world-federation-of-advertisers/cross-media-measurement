// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.ByteArray as CloudByteArray
import com.google.cloud.Timestamp
import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import liquibase.exception.CommandExecutionException
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.labeledOutputManifest

class UploadHealingOperationMigrationTest {
  @get:Rule val temporaryFolder = TemporaryFolder()
  @get:Rule val grpcCleanup = GrpcCleanupRule()

  @Test
  fun `migration backfills completed legacy rows and preserves scoped ids and steps`() =
    runBlocking<Unit> {
      withDatabase { fixture ->
        for ((provider, version) in listOf("dp-one" to null, "dp-two" to 1L)) {
          seedOperation(fixture.client, provider, OPERATION_ID, REQUEST_ID, 2L, version)
        }
        fixture.upgrade()

        for (provider in listOf("dp-one", "dp-two")) {
          assertThat(requestId(fixture.client, provider, OPERATION_ID)).isEqualTo(REQUEST_ID)
          val operation =
            reader(fixture.client)
              .getUploadHealingOperation(
                getUploadHealingOperationRequest {
                  dataProviderResourceId = provider
                  uploadHealingOperationId = OPERATION_ID
                }
              )
          assertThat(operation.planFormatVersion).isEqualTo(1)
          assertThat(operation.state)
            .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE)
          assertThat(operation.outputManifestState)
            .isEqualTo(
              UploadHealingOperation.OutputManifestState
                .UPLOAD_HEALING_OUTPUT_MANIFEST_STATE_FINALIZED
            )
          assertThat(operation.reason).isEqualTo("legacy correction")
          assertThat(operation.labeledImpressionsBlobPrefix).isEqualTo(OUTPUT_PREFIX)
          assertThat(operation.rawImpressionUploadCorrectionCandidateIdsList).isEmpty()
          val step = operation.stepsList.single()
          assertThat(step.sourceRawImpressionUploadResourceId).isEqualTo("bad-upload")
          assertThat(step.rawImpressionUploadModelLineResourceId).isEqualTo("model-line-row")
          assertThat(step.sequenceNumber).isEqualTo(1L)
          assertThat(step.state)
            .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE)
          assertThat(step.rawImpressionUploadCorrectionCandidateId).isEmpty()
          assertThat(step.labeledOutputManifest).isEqualTo(MANIFEST)
          val indexed =
            fixture.client
              .singleUse()
              .readRowUsingIndex(
                "UploadHealingOperation",
                "UploadHealingOperationByReconcileRequestId",
                Key.of(provider, REQUEST_ID),
                listOf("UploadHealingOperationId"),
              )
          assertThat(checkNotNull(indexed).getString("UploadHealingOperationId"))
            .isEqualTo(OPERATION_ID)
        }
        assertMigratedSchema(fixture.client)
        assertThat(migrationCount(fixture.client)).isEqualTo(1L)
        val duplicate =
          Mutation.newInsertBuilder("UploadHealingOperation")
            .set("DataProviderResourceId")
            .to("dp-one")
            .set("UploadHealingOperationId")
            .to(UUID.randomUUID().toString())
            .set("ReconcileRequestId")
            .to(REQUEST_ID)
            .set("State")
            .to(2L)
            .set("Reason")
            .to("duplicate")
            .set("LabeledImpressionsBlobPrefix")
            .to(OUTPUT_PREFIX)
            .set("CreateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        assertThat(assertFailsWith<SpannerException> { fixture.client.write(duplicate) }.errorCode)
          .isEqualTo(ErrorCode.ALREADY_EXISTS)
      }
    }

  @Test
  fun `migration permits an active candidate backed version two plan`() =
    runBlocking<Unit> {
      withDatabase { fixture ->
        seedOperation(fixture.client, "dp-one", OPERATION_ID, REQUEST_ID, 6L, 2L)
        fixture.upgrade()
        val operation =
          reader(fixture.client)
            .getUploadHealingOperation(
              getUploadHealingOperationRequest {
                dataProviderResourceId = "dp-one"
                uploadHealingOperationId = OPERATION_ID
              }
            )
        assertThat(operation.state)
          .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING)
        assertThat(operation.planFormatVersion).isEqualTo(2)
        assertThat(operation.rawImpressionUploadCorrectionCandidateIdsList)
          .containsExactly(CANDIDATE_ID)
        assertThat(operation.stepsList.single().rawImpressionUploadCorrectionCandidateId)
          .isEqualTo(CANDIDATE_ID)
        assertThat(operation.stepsList.single().labeledOutputManifest).isEqualTo(MANIFEST)
        assertThat(requestId(fixture.client, "dp-one", OPERATION_ID)).isEqualTo(REQUEST_ID)
        assertMigratedSchema(fixture.client)
      }
    }

  @Test
  fun `migration rejects every unfinished legacy state before changing schema and can be retried`() =
    runBlocking<Unit> {
      withDatabase { fixture ->
        seedOperation(fixture.client, "dp-one", OPERATION_ID, REQUEST_ID, 3L, null)
        for (version in listOf(null, 1L)) {
          for (state in 3L..10L) {
            // Keep exactly one unfinished row so no other variant can satisfy the guard.
            fixture.client.write(
              Mutation.newUpdateBuilder("UploadHealingOperation")
                .set("DataProviderResourceId")
                .to("dp-one")
                .set("UploadHealingOperationId")
                .to(OPERATION_ID)
                .set("State")
                .to(state)
                .set("PlanFormatVersion")
                .to(version)
                .set("OutputManifestState")
                .to(if (version != null) 2L else null)
                .build()
            )
            val before =
              fixture.client
                .singleUse()
                .executeQuery(Statement.of("SELECT * FROM UploadHealingOperation"))
                .toList()
            val error = assertFailsWith<CommandExecutionException> { fixture.upgrade() }
            assertThat(error).hasMessageThat().contains("SQL Precondition failed")
            assertThat(columns(fixture.client))
              .containsAtLeast("CreateRequestId", "BadRawImpressionUploadResourceIds", "CutoffTime")
            assertThat(columns(fixture.client)).doesNotContain("ReconcileRequestId")
            assertThat(indexes(fixture.client)).contains("UploadHealingOperationByCreateRequestId")
            assertThat(migrationCount(fixture.client)).isEqualTo(0L)
            assertThat(
                fixture.client
                  .singleUse()
                  .executeQuery(Statement.of("SELECT * FROM UploadHealingOperation"))
                  .toList()
              )
              .containsExactlyElementsIn(before)
          }
        }

        // Represent completion by the compatible old workflow, including its step.
        fixture.client.write(
          Mutation.newUpdateBuilder("UploadHealingOperation")
            .set("DataProviderResourceId")
            .to("dp-one")
            .set("UploadHealingOperationId")
            .to(OPERATION_ID)
            .set("State")
            .to(2L)
            .set("CompleteTime")
            .to(Value.COMMIT_TIMESTAMP)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build(),
          Mutation.newUpdateBuilder("UploadHealingStep")
            .set("DataProviderResourceId")
            .to("dp-one")
            .set("UploadHealingOperationId")
            .to(OPERATION_ID)
            .set("UploadHealingStepId")
            .to(1L)
            .set("CompleteTime")
            .to(Value.COMMIT_TIMESTAMP)
            .set("UpdateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build(),
        )
        fixture.upgrade()
        assertMigratedSchema(fixture.client)
        assertThat(migrationCount(fixture.client)).isEqualTo(1L)
        assertThat(requestId(fixture.client, "dp-one", OPERATION_ID)).isEqualTo(REQUEST_ID)
      }
    }

  @Test
  fun `migration applies to an empty database`() =
    runBlocking<Unit> {
      withDatabase { fixture ->
        fixture.upgrade()
        assertMigratedSchema(fixture.client)
        assertThat(migrationCount(fixture.client)).isEqualTo(1L)
      }
    }

  private suspend fun withDatabase(block: suspend (Fixture) -> Unit) {
    val directory = temporaryFolder.newFolder().toPath()
    val source = Schemata.EDP_AGGREGATOR_CHANGELOG_PATH
    Files.list(source.parent).use { files ->
      files
        .filter { Files.isRegularFile(it) }
        .forEach { Files.copy(it, directory.resolve(it.fileName.toString())) }
    }
    val changelog = directory.resolve(source.fileName.toString())
    val full = Files.readString(changelog)
    val include =
      "  - include:\n      file: remove-obsolete-upload-healing-fields.sql\n      relativeToChangeLogFile: true\n"
    val migrationOffset = full.indexOf(include)
    check(migrationOffset >= 0) { "Obsolete-field migration include is missing" }
    Files.writeString(changelog, full.substring(0, migrationOffset))
    val id = "migration-" + UUID.randomUUID().toString().take(8)
    val client = emulator.createDatabase(changelog, id)
    try {
      block(Fixture(client, changelog, full, id))
    } finally {
      emulator.deleteDatabase(id)
    }
  }

  private data class Fixture(
    val client: AsyncDatabaseClient,
    val changelog: Path,
    val full: String,
    val id: String,
  ) {
    fun upgrade() {
      Files.writeString(changelog, full)
      emulator.createDatabase(changelog, id)
    }
  }

  private suspend fun seedOperation(
    client: AsyncDatabaseClient,
    provider: String,
    id: String,
    request: String,
    state: Long,
    version: Long?,
  ) {
    val complete = state == 2L
    val operation =
      Mutation.newInsertBuilder("UploadHealingOperation")
        .set("DataProviderResourceId")
        .to(provider)
        .set("UploadHealingOperationId")
        .to(id)
        .set("CreateRequestId")
        .to(request)
        .set("State")
        .to(state)
        .set("PlanFormatVersion")
        .to(version)
        .set("OutputManifestState")
        .to(if (version != null) 2L else null)
        .set("Reason")
        .to("legacy correction")
        .set("LabeledImpressionsBlobPrefix")
        .to(OUTPUT_PREFIX)
        .set("BadRawImpressionUploadResourceIds")
        .toStringArray(listOf("bad-upload"))
        .set("CutoffTime")
        .to(Timestamp.parseTimestamp("2026-01-01T00:00:00Z"))
        .set("RawImpressionUploadCorrectionCandidateIds")
        .toStringArray(if (version == 2L) listOf(CANDIDATE_ID) else emptyList())
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("CompleteTime")
        .to(if (complete) Value.COMMIT_TIMESTAMP else null)
        .build()
    val step =
      Mutation.newInsertBuilder("UploadHealingStep")
        .set("DataProviderResourceId")
        .to(provider)
        .set("UploadHealingOperationId")
        .to(id)
        .set("UploadHealingStepId")
        .to(1L)
        .set("SequenceNumber")
        .to(1L)
        .set("SourceRawImpressionUploadResourceId")
        .to("bad-upload")
        .set("RawImpressionUploadModelLineResourceId")
        .to("model-line-row")
        .set("CmmsModelLine")
        .to("modelProviders/mp/modelSuites/ms/modelLines/ml")
        .set("Memoized")
        .to(true)
        .set("RecoveryAction")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineRecoveryAction
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
          )
        )
        .set("RecoveryTarget")
        .to(true)
        .set("RawImpressionUploadCorrectionCandidateId")
        .to(if (version == 2L) CANDIDATE_ID else null)
        .set("LabeledOutputManifest")
        .to(CloudByteArray.copyFrom(MANIFEST.toByteArray()))
        .set("CompleteTime")
        .to(if (complete) Value.COMMIT_TIMESTAMP else null)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    client.write(operation, step)
  }

  private fun reader(client: AsyncDatabaseClient): UploadHealingOperationServiceCoroutineStub {
    val name = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(name)
        .directExecutor()
        .addService(SpannerUploadHealingOperationService(client))
        .build()
        .start()
    )
    val channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(name).directExecutor().build())
    return UploadHealingOperationServiceCoroutineStub(channel)
  }

  private suspend fun requestId(client: AsyncDatabaseClient, provider: String, id: String): String =
    checkNotNull(
        client
          .singleUse()
          .readRow("UploadHealingOperation", Key.of(provider, id), listOf("ReconcileRequestId"))
      )
      .getString("ReconcileRequestId")

  private suspend fun columns(client: AsyncDatabaseClient): List<String> =
    client
      .singleUse()
      .executeQuery(
        Statement.of(
          "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'UploadHealingOperation'"
        )
      )
      .toList()
      .map { it.getString("COLUMN_NAME") }

  private suspend fun indexes(client: AsyncDatabaseClient): List<String> =
    client
      .singleUse()
      .executeQuery(
        Statement.of(
          "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = 'UploadHealingOperation'"
        )
      )
      .toList()
      .map { it.getString("INDEX_NAME") }

  private suspend fun migrationCount(client: AsyncDatabaseClient): Long =
    client
      .singleUse()
      .executeQuery(
        Statement.of(
          "SELECT COUNT(*) AS n FROM DATABASECHANGELOG WHERE ID = '26' AND AUTHOR = 'marcopremier'"
        )
      )
      .single()
      .getLong("n")

  private suspend fun assertMigratedSchema(client: AsyncDatabaseClient) {
    assertThat(columns(client)).contains("ReconcileRequestId")
    assertThat(columns(client))
      .containsNoneOf("CreateRequestId", "BadRawImpressionUploadResourceIds", "CutoffTime")
    assertThat(indexes(client)).contains("UploadHealingOperationByReconcileRequestId")
    assertThat(indexes(client)).doesNotContain("UploadHealingOperationByCreateRequestId")
  }

  companion object {
    @get:ClassRule @JvmStatic val emulator = SpannerEmulatorRule()
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val CANDIDATE_ID = "33333333-3333-4333-8333-333333333333"
    private const val OUTPUT_PREFIX = "gs://output/vid"
    private val MANIFEST = labeledOutputManifest {
      blobs +=
        LabeledOutputManifestKt.blobVersion {
          blobUri = "gs://output/vid/data"
          generation = 42L
        }
    }
  }
}
