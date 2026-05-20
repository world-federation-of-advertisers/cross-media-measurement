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
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import java.io.File
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common as MarketCommon
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.MarketEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
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

  @Test
  fun `resolveEventMessageInstance returns compiled TestEvent for the default type URL`() {
    val instance: Message =
      GenerateSyntheticData.resolveEventMessageInstance(
        GenerateSyntheticData.DEFAULT_EVENT_MESSAGE_TYPE_URL,
        emptyList(),
      )
    assertThat(instance).isInstanceOf(TestEvent::class.java)
  }

  @Test
  fun `resolveEventMessageInstance returns DynamicMessage from descriptor set for non-default type URL`() {
    // Build a FileDescriptorSet covering all transitive dependencies of TestEvent and write it to
    // disk. Then resolve a *non-default* type URL that points at one of the included messages
    // (Person). This exercises the dynamic-message code path that supports arbitrary user-supplied
    // event message types.
    val descriptorSetFile = tempFolder.newFile("test_event_descriptor_set.binpb")
    val descriptorSetBuilder = DescriptorProtos.FileDescriptorSet.newBuilder()
    for (file in collectFiles(TestEvent.getDescriptor().file)) {
      descriptorSetBuilder.addFile(file.toProto())
    }
    descriptorSetFile.writeBytes(descriptorSetBuilder.build().toByteArray())

    val instance: Message =
      GenerateSyntheticData.resolveEventMessageInstance(
        "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.Person",
        listOf(descriptorSetFile),
      )

    assertThat(instance).isInstanceOf(DynamicMessage::class.java)
    assertThat(instance.descriptorForType.fullName)
      .isEqualTo("wfa.measurement.api.v2alpha.event_templates.testing.Person")
  }

  @Test
  fun `resolveEventMessageInstance throws when type URL is not in descriptor set`() {
    val emptyDescriptorSetFile = tempFolder.newFile("empty_descriptor_set.binpb")
    emptyDescriptorSetFile.writeBytes(
      DescriptorProtos.FileDescriptorSet.getDefaultInstance().toByteArray()
    )
    assertFailsWith<IllegalStateException> {
      GenerateSyntheticData.resolveEventMessageInstance(
        "type.googleapis.com/some.unknown.package.UnknownEvent",
        listOf(emptyDescriptorSetFile),
      )
    }
  }

  @Test
  fun `resolveEventMessageInstance throws when descriptor set is missing for non-default type URL`() {
    assertFailsWith<IllegalArgumentException> {
      GenerateSyntheticData.resolveEventMessageInstance(
        "type.googleapis.com/some.other.Event",
        emptyList(),
      )
    }
  }

  @Test
  fun `generate succeeds with explicitly supplied default event-message-type-url flag`() {
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
          // Explicitly supply the type URL flag (defaults to the same value).
          "--event-message-type-url=${GenerateSyntheticData.DEFAULT_EVENT_MESSAGE_TYPE_URL}",
          "--event-group-reference-id=${SPEC_A.eventGroupReferenceId}",
          "--data-spec-resource-path=${SPEC_A.dataSpecResourcePath}",
          "--entity-key-type=${SPEC_A.entityKeyType}",
          "--entity-key-id=${SPEC_A.entityKeyIds[0]}",
        )
    assertThat(exitCode).isEqualTo(0)

    // Verify by running VerifySyntheticData with the explicit flag too.
    val verifyCmd = VerifySyntheticData()
    val verifyExitCode =
      CommandLine(verifyCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--output-bucket=$OUTPUT_BUCKET",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
          "--event-message-type-url=${GenerateSyntheticData.DEFAULT_EVENT_MESSAGE_TYPE_URL}",
        )
    assertThat(verifyExitCode).isEqualTo(0)
    assertThat(verifyCmd.lastResult!!.errors).isEqualTo(0)
    assertThat(verifyCmd.lastResult!!.totalImpressions).isEqualTo(SPEC_A.expectedImpressions)
  }

  @Test
  fun `generate and verify with non-TestEvent MarketEvent via descriptor set`() {
    val outputBucketDir = tempFolder.root.resolve(OUTPUT_BUCKET)
    outputBucketDir.mkdirs()

    val descriptorSetFile = MARKET_EVENT_DESCRIPTOR_SET_RUNTIME_PATH.toFile()
    check(descriptorSetFile.exists()) {
      "MarketEvent descriptor set runfile not found at $descriptorSetFile"
    }

    val generateCmd = GenerateSyntheticData()
    val generateExitCode =
      CommandLine(generateCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--model-line=$MODEL_LINE",
          "--output-bucket=$OUTPUT_BUCKET",
          "--schema=file:///",
          "--population-spec-resource-path=$MARKET_POPULATION_SPEC",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
          "--event-message-type-url=$MARKET_EVENT_TYPE_URL",
          "--event-message-descriptor-set=${descriptorSetFile.path}",
          "--event-group-reference-id=eg-market",
          "--data-spec-resource-path=$MARKET_DATA_SPEC",
          "--entity-key-type=creative",
          "--entity-key-id=creative-market",
        )
    assertThat(generateExitCode).isEqualTo(0)

    // Verify with the same descriptor flags. VerifySyntheticData should validate that the
    // impression event type URLs match the supplied --event-message-type-url and that each
    // event payload parses cleanly into the dynamic message instance.
    val verifyCmd = VerifySyntheticData()
    val verifyExitCode =
      CommandLine(verifyCmd)
        .execute(
          "--kms-type=FAKE",
          "--kek-uri=$KEK_URI",
          "--fake-kek-keyset-file=${fakeKekKeysetFile().path}",
          "--local-storage-path=${tempFolder.root.path}",
          "--output-bucket=$OUTPUT_BUCKET",
          "--impression-metadata-base-path=$IMPRESSION_METADATA_BASE_PATH",
          "--event-message-type-url=$MARKET_EVENT_TYPE_URL",
          "--event-message-descriptor-set=${descriptorSetFile.path}",
        )
    assertThat(verifyExitCode).isEqualTo(0)
    val result = verifyCmd.lastResult!!
    assertThat(result.errors).isEqualTo(0)
    assertThat(result.totalImpressions).isEqualTo(MARKET_EVENT_EXPECTED_IMPRESSIONS)
    assertThat(result.impressionsByEventGroupReferenceId)
      .containsExactly("eg-market", MARKET_EVENT_EXPECTED_IMPRESSIONS)

    // Independently re-decrypt one impression and parse it as a compiled MarketEvent to confirm
    // the bytes are valid wire form for the real MarketEvent message and carry the expected
    // attribute values.
    val kmsClient = GenerateSyntheticData.buildFakeKmsClient(KEK_URI, fakeKekKeysetFile())
    val modelLineId = MODEL_LINE.substringAfterLast('/')
    val perEventGroupRelativeDir =
      "$IMPRESSION_METADATA_BASE_PATH/ds/2024-01-01/" +
        "model-line/$modelLineId/event-group-reference-id/eg-market"
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val blobDetails = runBlocking {
      BlobDetails.parseFrom(
        storageClient
          .getBlob("$OUTPUT_BUCKET/$perEventGroupRelativeDir/metadata.binpb")!!
          .read()
          .flatten()
      )
    }
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
    val sampleImpression = impressions.first()
    assertThat(sampleImpression.event.typeUrl).isEqualTo(MARKET_EVENT_TYPE_URL)
    val parsedEvent = MarketEvent.parseFrom(sampleImpression.event.value)
    assertThat(parsedEvent.common.sex).isNotEqualTo(MarketCommon.Sex.SEX_UNSPECIFIED)
    assertThat(parsedEvent.common.ageGroup)
      .isNotEqualTo(MarketCommon.AgeGroup.AGE_GROUP_UNSPECIFIED)

    // Verify counts grouped by (Common.sex, Common.age_group) across all impressions.
    val allImpressions: List<LabeledImpression> = decryptAllImpressions()
    val parsedEvents: List<MarketEvent> =
      allImpressions.map { MarketEvent.parseFrom(it.event.value) }

    val countsBySexAndAge: Map<Pair<MarketCommon.Sex, MarketCommon.AgeGroup>, Int> =
      parsedEvents.groupingBy { it.common.sex to it.common.ageGroup }.eachCount()

    // Expected counts derived from small_market_population_spec.textproto +
    // small_market_data_spec.textproto:
    //   VID 1..200      -> sub-pop 1 (MALE,   16-34) freq 1 -> 200
    //   VID 10001..10100-> sub-pop 2 (MALE,   35-54) freq 2 -> 200
    //   VID 20001..20050-> sub-pop 3 (FEMALE, 16-34) freq 3 -> 150
    assertThat(countsBySexAndAge)
      .containsExactly(
        MarketCommon.Sex.MALE to MarketCommon.AgeGroup.YEARS_16_TO_34,
        200,
        MarketCommon.Sex.MALE to MarketCommon.AgeGroup.YEARS_35_TO_54,
        200,
        MarketCommon.Sex.FEMALE to MarketCommon.AgeGroup.YEARS_16_TO_34,
        150,
      )
  }

  @Test
  fun `verify TestEvent impression counts grouped by gender and age_group`() {
    runGenerate()

    val impressions: List<LabeledImpression> = decryptAllImpressions()
    val testEvents: List<TestEvent> = impressions.map { TestEvent.parseFrom(it.event.value) }

    val countsByGenderAndAge: Map<Pair<Person.Gender, Person.AgeGroup>, Int> =
      testEvents.groupingBy { it.person.gender to it.person.ageGroup }.eachCount()

    // Expected counts derived from small_population_spec.textproto +
    // small_data_spec.textproto + small_data_spec_b.textproto:
    //
    //   small_data_spec.textproto (event group eg-a, 8001 impressions):
    //     VID 1..2000     -> sub-pop 1  (MALE,   18-34) freq 1+2 -> 1000+2000 = 3000
    //     VID 20001..22000-> sub-pop 3  (MALE,   35-54) freq 1+2 -> 1000+2000 = 3000
    //     VID 91001..92000-> sub-pop 11 (FEMALE, 55+)   freq 1   -> 1000
    //     VID 98000..99000-> sub-pop 12 (FEMALE, 55+)   freq 1   -> 1001
    //
    //   small_data_spec_b.textproto (event group eg-b, 500 impressions):
    //     VID 50001..50200-> sub-pop 6  (MALE,   55+)   freq 1   -> 200
    //     VID 60001..60100-> sub-pop 7  (FEMALE, 18-34) freq 3   -> 300
    assertThat(countsByGenderAndAge)
      .containsExactly(
        Person.Gender.MALE to Person.AgeGroup.YEARS_18_TO_34,
        3000,
        Person.Gender.MALE to Person.AgeGroup.YEARS_35_TO_54,
        3000,
        Person.Gender.MALE to Person.AgeGroup.YEARS_55_PLUS,
        200,
        Person.Gender.FEMALE to Person.AgeGroup.YEARS_18_TO_34,
        300,
        Person.Gender.FEMALE to Person.AgeGroup.YEARS_55_PLUS,
        1000 + 1001,
      )
    assertThat(testEvents).hasSize(SPEC_A.expectedImpressions + SPEC_B.expectedImpressions)
  }

  @Test
  fun `verify reports errors when event-message-type-url does not match impression type URL`() {
    runGenerate()

    val descriptorSetFile = MARKET_EVENT_DESCRIPTOR_SET_RUNTIME_PATH.toFile()
    check(descriptorSetFile.exists()) {
      "MarketEvent descriptor set runfile not found at $descriptorSetFile"
    }

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
          // Impressions on disk were written with the TestEvent type URL but we deliberately ask
          // the verifier to expect MarketEvent. The resolver will succeed (the descriptor set
          // contains MarketEvent), so the failure must come from the per-impression type URL
          // check inside verifySyntheticData(). Every blob (= EXPECTED_DATES.size * 2) must be
          // marked as an error and totalImpressions must remain 0.
          "--event-message-type-url=$MARKET_EVENT_TYPE_URL",
          "--event-message-descriptor-set=${descriptorSetFile.path}",
        )

    // The CLI exits 0 even when verification reports errors (errors are reported via lastResult).
    assertThat(exitCode).isEqualTo(0)
    val result = verifyCmd.lastResult!!
    assertThat(result.errors).isEqualTo(EXPECTED_DATES.size * 2)
    assertThat(result.totalBlobsProcessed).isEqualTo(0)
    assertThat(result.totalImpressions).isEqualTo(0)
    assertThat(result.impressionsByEventGroupReferenceId).isEmpty()
  }

  /**
   * Decrypts every impressions blob produced by the most recent [runGenerate] (or equivalent)
   * invocation, returning all [LabeledImpression]s flattened across dates and event groups.
   */
  private fun decryptAllImpressions(): List<LabeledImpression> {
    val kmsClient = GenerateSyntheticData.buildFakeKmsClient(KEK_URI, fakeKekKeysetFile())
    val rootStorage = FileSystemStorageClient(tempFolder.root)
    val dsDir =
      tempFolder.root.resolve(OUTPUT_BUCKET).resolve(IMPRESSION_METADATA_BASE_PATH).resolve("ds")
    check(dsDir.exists()) { "Impressions output not found under $dsDir" }

    return dsDir
      .walkTopDown()
      .filter { it.name == "metadata.binpb" }
      .flatMap { metadataFile ->
        val relativeMetadataPath =
          metadataFile.relativeTo(tempFolder.root.resolve(OUTPUT_BUCKET)).path
        val blobDetails = runBlocking {
          BlobDetails.parseFrom(
            rootStorage.getBlob("$OUTPUT_BUCKET/$relativeMetadataPath")!!.read().flatten()
          )
        }
        val impressionsBlobKey =
          blobDetails.blobUri.removePrefix("file:///").removePrefix("$OUTPUT_BUCKET/")
        val selectedStorageClient = SelectedStorageClient(blobDetails.blobUri, tempFolder.root)
        val decryptingStorage =
          selectedStorageClient.withEnvelopeEncryption(
            kmsClient,
            KEK_URI,
            blobDetails.encryptedDek.ciphertext,
          )
        runBlocking {
          MesosRecordIoStorageClient(decryptingStorage)
            .getBlob(impressionsBlobKey)!!
            .read()
            .toList()
            .map { LabeledImpression.parseFrom(it) }
        }
      }
      .toList()
  }

  /**
   * Recursively collects [com.google.protobuf.Descriptors.FileDescriptor] dependencies starting
   * from [root].
   */
  private fun collectFiles(
    root: com.google.protobuf.Descriptors.FileDescriptor
  ): Set<com.google.protobuf.Descriptors.FileDescriptor> {
    val out = LinkedHashSet<com.google.protobuf.Descriptors.FileDescriptor>()
    fun visit(file: com.google.protobuf.Descriptors.FileDescriptor) {
      if (out.add(file)) {
        for (dep in file.dependencies) {
          visit(dep)
        }
      }
    }
    visit(root)
    return out
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

    /**
     * Type URL for the test-only [MarketEvent] message
     * (`wfa.measurement.api.v2alpha.event_templates.testing.market.v1.MarketEvent`). Used to
     * exercise GenerateSyntheticData/VerifySyntheticData against a non-`TestEvent` message type.
     */
    private const val MARKET_EVENT_TYPE_URL =
      "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.market.v1.MarketEvent"

    /**
     * v2alpha PopulationSpec textproto under [TEST_DATA_PATH] sized to match [MARKET_DATA_SPEC];
     * references the MarketEvent `Common` attribute message.
     */
    private const val MARKET_POPULATION_SPEC = "small_market_population_spec.textproto"

    /**
     * SyntheticEventGroupSpec textproto under [TEST_DATA_PATH] using non-population field paths
     * exposed by the MarketEvent `Video` and `Display` templates.
     */
    private const val MARKET_DATA_SPEC = "small_market_data_spec.textproto"

    /**
     * Total impressions produced by [MARKET_DATA_SPEC]: (200 VIDs * 1) + (100 VIDs * 2) + (50 VIDs
     * * 3) = 550.
     */
    private const val MARKET_EVENT_EXPECTED_IMPRESSIONS = 550

    /**
     * Runtime path of the [proto_descriptor_set]-generated FileDescriptorSet for the test-only
     * [MarketEvent] message and its template/dependency files. The file is added to the test
     * target's `data` deps and resolved through the Bazel runfiles tree.
     */
    private val MARKET_EVENT_DESCRIPTOR_SET_RUNTIME_PATH =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "proto",
          "wfa",
          "measurement",
          "api",
          "v2alpha",
          "event_templates",
          "testing",
          "market",
          "v1",
          "market_event_descriptor_set.pb",
        )
      )!!
  }
}
