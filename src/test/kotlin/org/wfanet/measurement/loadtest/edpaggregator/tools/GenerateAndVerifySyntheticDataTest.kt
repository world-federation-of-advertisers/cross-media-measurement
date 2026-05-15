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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.io.File
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine

/**
 * End-to-end tests for [GenerateSyntheticData] and [VerifySyntheticData] driven through their
 * picocli command line surface, covering the multi-event-group flow where a single CLI invocation
 * coalesces multiple
 * [`SyntheticEventGroupSpec`][org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec]
 * inputs into a single tree of encrypted impression blobs.
 */
@RunWith(JUnit4::class)
class GenerateAndVerifySyntheticDataTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  /** Runs `GenerateSyntheticData` against [tempFolder] with both [SPEC_A] and [SPEC_B]. */
  private fun runGenerate(): GenerateSyntheticData {
    val outputBucketDir = tempFolder.root.resolve(OUTPUT_BUCKET)
    outputBucketDir.mkdirs()

    val generateCmd = GenerateSyntheticData()
    val exitCode =
      CommandLine(generateCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--model-line=$MODEL_LINE",
          "--output-bucket=$OUTPUT_BUCKET",
          "--schema=file:///",
          "--population-spec-resource-path=$POPULATION_SPEC",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
          "--event-group-reference-id=${SPEC_A.eventGroupReferenceId}",
          "--data-spec-resource-path=${SPEC_A.dataSpecResourcePath}",
          "--entity-key-type=${SPEC_A.entityKeyType}",
          "--entity-key-id=${SPEC_A.entityKeyIds[0]}",
          "--entity-key-type=${SPEC_A.entityKeyType}",
          "--entity-key-id=${SPEC_A.entityKeyIds[1]}",
          "--event-group-reference-id=${SPEC_B.eventGroupReferenceId}",
          "--data-spec-resource-path=${SPEC_B.dataSpecResourcePath}",
          "--entity-key-type=${SPEC_B.entityKeyType}",
          "--entity-key-id=${SPEC_B.entityKeyIds[0]}",
        )
    check(exitCode == 0) { "GenerateSyntheticData exited with code $exitCode" }
    return generateCmd
  }

  /** Path to the fake KEK keyset shared by Generate and Verify within a single test. */
  private fun fakeKekKeysetFile(): File = tempFolder.root.resolve("fake-kek.tinkkeyset")

  @Test
  fun `generate with multiple specs produces coalesced output for both event groups`() {
    runGenerate()

    val dsDir =
      tempFolder.root.resolve(OUTPUT_BUCKET).resolve(IMPRESSION_METADATA_BASE_PATH).resolve("ds")
    assertThat(dsDir.exists()).isTrue()

    val dateDirs = dsDir.listFiles()!!.filter { it.isDirectory }.map { it.name }.sorted()
    assertThat(dateDirs).isEqualTo(EXPECTED_DATES)

    // Each date must contain BOTH event group subdirectories, each with metadata + impressions.
    val modelLineId = MODEL_LINE.substringAfterLast('/')
    for (date in EXPECTED_DATES) {
      for (eventGroupReferenceId in
        listOf(SPEC_A.eventGroupReferenceId, SPEC_B.eventGroupReferenceId)) {
        val perEventGroupDir =
          dsDir
            .resolve(date)
            .resolve("model-line/$modelLineId/event-group-reference-id/$eventGroupReferenceId")
        assertThat(perEventGroupDir.resolve("metadata.binpb").exists()).isTrue()
        assertThat(perEventGroupDir.resolve("impressions").exists()).isTrue()
      }
    }

    val metadataFileCount = dsDir.walkTopDown().count { it.name == "metadata.binpb" }
    assertThat(metadataFileCount).isEqualTo(EXPECTED_DATES.size * 2)
  }

  @Test
  fun `verify decrypts all impressions from both event group streams`() {
    runGenerate()

    val verifyCmd = VerifySyntheticData()
    val exitCode =
      CommandLine(verifyCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--output-bucket=$OUTPUT_BUCKET",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
        )
    assertThat(exitCode).isEqualTo(0)

    val result = verifyCmd.lastResult!!
    assertThat(result.errors).isEqualTo(0)
    assertThat(result.totalBlobsProcessed).isEqualTo(EXPECTED_DATES.size * 2)
    assertThat(result.totalImpressions)
      .isEqualTo(SPEC_A.expectedImpressions + SPEC_B.expectedImpressions)
    assertThat(result.impressionsByEventGroupReferenceId)
      .containsExactly(
        SPEC_A.eventGroupReferenceId,
        SPEC_A.expectedImpressions,
        SPEC_B.eventGroupReferenceId,
        SPEC_B.expectedImpressions,
      )
  }

  @Test
  fun `generate with multiple sub-specs per event group stamps different EntityKeys in one blob`() {
    val outputBucketDir = tempFolder.root.resolve(OUTPUT_BUCKET)
    outputBucketDir.mkdirs()

    val generateCmd = GenerateSyntheticData()
    val exitCode =
      CommandLine(generateCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--model-line=$MODEL_LINE",
          "--output-bucket=$OUTPUT_BUCKET",
          "--schema=file:///",
          "--population-spec-resource-path=$POPULATION_SPEC",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
          // Single event group, two sub-specs.
          "--event-group-reference-id=eg-mixed",
          "--data-spec-resource-path=${SPEC_A.dataSpecResourcePath}",
          "--entity-key-type=creative",
          "--entity-key-id=creative-A1",
          "--data-spec-resource-path=${SPEC_B.dataSpecResourcePath}",
          "--entity-key-type=creative",
          "--entity-key-id=creative-B1",
        )
    check(exitCode == 0) { "GenerateSyntheticData exited with code $exitCode" }

    val kmsClient = GenerateSyntheticData.buildFakeKmsClient(KEK_URI, fakeKekKeysetFile())
    val modelLineId = MODEL_LINE.substringAfterLast('/')
    val perEventGroupRelativeDir =
      "$IMPRESSION_METADATA_BASE_PATH/ds/$INSPECT_DATE/" +
        "model-line/$modelLineId/event-group-reference-id/eg-mixed"
    val storageClient = FileSystemStorageClient(tempFolder.root)

    val blobDetails = runBlocking {
      BlobDetails.parseFrom(
        storageClient
          .getBlob("$OUTPUT_BUCKET/$perEventGroupRelativeDir/metadata.binpb")!!
          .read()
          .flatten()
      )
    }

    assertThat(blobDetails.eventGroupReferenceId).isEqualTo("eg-mixed")
    assertThat(blobDetails.modelLine).isEqualTo(MODEL_LINE)

    val selectedStorageClient = SelectedStorageClient(blobDetails.blobUri, tempFolder.root)
    val decryptingStorage =
      selectedStorageClient.withEnvelopeEncryption(
        kmsClient,
        KEK_URI,
        blobDetails.encryptedDek.ciphertext,
      )
    val impressions: List<LabeledImpression> = runBlocking {
      MesosRecordIoStorageClient(decryptingStorage)
        .getBlob("$perEventGroupRelativeDir/impressions")!!
        .read()
        .toList()
        .map { LabeledImpression.parseFrom(it) }
    }
    assertThat(impressions).isNotEmpty()

    // Every impression should have exactly one EntityKey with id either creative-A1 or creative-B1,
    // and BOTH ids must appear in the blob (within-file variation).
    val seenIds = mutableSetOf<String>()
    for (impression in impressions) {
      assertThat(impression.entityKeysList).hasSize(1)
      val entityKey = impression.entityKeysList.single()
      assertThat(entityKey.entityType).isEqualTo("creative")
      assertThat(entityKey.entityId).isAnyOf("creative-A1", "creative-B1")
      seenIds.add(entityKey.entityId)
    }
    assertThat(seenIds).containsExactly("creative-A1", "creative-B1")
  }

  /** Hand-rolled mini event-group-spec descriptor used to drive both flags and assertions. */
  private data class TestEventGroupSpec(
    val eventGroupReferenceId: String,
    val dataSpecResourcePath: String,
    val entityKeyType: String,
    val entityKeyIds: List<String>,
    /** Total impressions the data-spec textproto is expected to produce. */
    val expectedImpressions: Int,
  )

  companion object {
    @BeforeClass
    @JvmStatic
    fun registerTink() {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "key1"
    private const val MODEL_LINE =
      "modelProviders/provider1/modelSuites/suite1/modelLines/some-model-line"
    private const val OUTPUT_BUCKET = "test-bucket"
    private const val POPULATION_SPEC = "small_population_spec.textproto"
    private const val IMPRESSION_METADATA_BASE_PATH = "run1"
    private const val INSPECT_DATE = "2021-03-15"

    /** small_data_spec.textproto: 8001 impressions across 2021-03-15..2021-03-21. */
    private val SPEC_A =
      TestEventGroupSpec(
        eventGroupReferenceId = "eg-a",
        dataSpecResourcePath = "small_data_spec.textproto",
        entityKeyType = "creative",
        entityKeyIds = listOf("creative-A1", "creative-A2"),
        expectedImpressions = 8001,
      )

    /** small_data_spec_b.textproto: 500 impressions over the same date range. */
    private val SPEC_B =
      TestEventGroupSpec(
        eventGroupReferenceId = "eg-b",
        dataSpecResourcePath = "small_data_spec_b.textproto",
        entityKeyType = "creative",
        entityKeyIds = listOf("creative-B1"),
        expectedImpressions = 500,
      )

    /** Date range shared by both specs (March 15 through March 21 inclusive, end-exclusive 22). */
    private val EXPECTED_DATES =
      listOf(
        "2021-03-15",
        "2021-03-16",
        "2021-03-17",
        "2021-03-18",
        "2021-03-19",
        "2021-03-20",
        "2021-03-21",
      )
  }
}
