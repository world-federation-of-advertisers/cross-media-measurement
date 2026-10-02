/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate

@RunWith(JUnit4::class)
class RawImpressionUploadCorrectionCandidateMigrationTest {
  @Test
  fun `manifest comparison migration converts legacy candidate to manual intervention`() =
    runBlocking {
      val partialChangelogDirectory = Files.createTempDirectory("candidate-migration")
      val databaseId = "candidate-migration"
      var databaseCreated = false
      try {
        val partialChangelog = copyChangelogThroughCandidateSchema(partialChangelogDirectory)
        val databaseClient = spannerEmulator.createDatabase(partialChangelog, databaseId)
        databaseCreated = true
        databaseClient.write(listOf(legacyCandidateMutation()))

        val upgradedDatabaseClient =
          spannerEmulator.createDatabase(
            manifestComparisonMigrationChangelog(partialChangelogDirectory),
            databaseId,
          )
        val candidate =
          upgradedDatabaseClient.singleUse().use {
            checkNotNull(
                it.findRawImpressionUploadCorrectionCandidate(DATA_PROVIDER_ID, CANDIDATE_ID)
              )
              .rawImpressionUploadCorrectionCandidate
          }

        assertThat(candidate.state)
          .isEqualTo(
            RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
          )
        assertThat(candidate.manifestComparison)
          .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestComparison.getDefaultInstance())
      } finally {
        if (databaseCreated) {
          spannerEmulator.deleteDatabase(databaseId)
        }
        partialChangelogDirectory.toFile().deleteRecursively()
      }
    }

  private fun copyChangelogThroughCandidateSchema(directory: Path): Path {
    val fullChangelog = Schemata.EDP_AGGREGATOR_CHANGELOG_PATH
    Files.list(fullChangelog.parent).use { files ->
      files
        .filter { Files.isRegularFile(it) }
        .forEach { source ->
          Files.copy(
            source,
            directory.resolve(source.fileName.toString()),
            StandardCopyOption.REPLACE_EXISTING,
          )
        }
    }
    val partialChangelog = directory.resolve(fullChangelog.fileName.toString())
    val fullContents = Files.readString(partialChangelog)
    check(MANIFEST_COMPARISON_MIGRATION_INCLUDE in fullContents)
    Files.writeString(
      partialChangelog,
      fullContents.substringBefore(MANIFEST_COMPARISON_MIGRATION_INCLUDE).trimEnd() + "\n",
    )
    return partialChangelog
  }

  private fun manifestComparisonMigrationChangelog(directory: Path): Path {
    val changelog = directory.resolve("manifest-comparison-changelog.yaml")
    Files.writeString(
      changelog,
      """
      databaseChangeLog:
        - include:
            file: add-raw-impression-upload-correction-candidate-manifest-comparison.sql
            relativeToChangeLogFile: true
      """
        .trimIndent() + "\n",
    )
    return changelog
  }

  private fun legacyCandidateMutation(): Mutation =
    Mutation.newInsertBuilder("RawImpressionUploadCorrectionCandidate")
      .set("DataProviderResourceId")
      .to(DATA_PROVIDER_ID)
      .set("RawImpressionUploadCorrectionCandidateId")
      .to(CANDIDATE_ID)
      .set("RawImpressionUploadResourceId")
      .to(UPLOAD_ID)
      .set("CreateRequestId")
      .to(CREATE_REQUEST_ID)
      .set("Classification")
      .to(
        Value.protoEnum(RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED)
      )
      .set("PriorManifestDigest")
      .to(com.google.cloud.ByteArray.copyFrom(ByteArray(32) { 1 }))
      .set("CurrentManifestDigest")
      .to(com.google.cloud.ByteArray.copyFrom(ByteArray(32) { 2 }))
      .set("State")
      .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING))
      .set("Decision")
      .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED))
      .set("ExpireTime")
      .to(Timestamp.ofTimeSecondsAndNanos(4_102_444_800L, 0))
      .set("AdvanceRequestIds")
      .toStringArray(emptyList())
      .set("AdvanceRequestFingerprints")
      .toBytesArray(emptyList())
      .set("CreateTime")
      .to(Value.COMMIT_TIMESTAMP)
      .set("UpdateTime")
      .to(Value.COMMIT_TIMESTAMP)
      .build()

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val UPLOAD_ID = "upload"
    private const val CREATE_REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val MANIFEST_COMPARISON_MIGRATION_INCLUDE =
      "  - include:\n" +
        "      file: add-raw-impression-upload-correction-candidate-manifest-comparison.sql"
  }
}
