/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.nio.file.Files
import java.time.LocalDate
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

@RunWith(JUnit4::class)
class RequisitionSpecsTest {

  @get:Rule val tempFolder = TemporaryFolder()

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Test
  fun `getSampledVids filters impressions correctly`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event group entry with filter
    val eventGroup = eventGroupEntry {
      key = EVENT_GROUP_NAME
      value = RequisitionSpecKt.EventGroupEntryKt.value {
        this.collectionInterval = collectionInterval
        filter = eventFilter { expression = "person.gender == 1" } // MALE is 1
      }
    }

    // Create requisition spec with event group
    val requisitionSpec = requisitionSpec {
      events = events {
        eventGroups += eventGroup
      }
    }

    // Create sampling interval
    val vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 1.0f
    }

    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(impressionsTmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, impressionsTmpPath)

    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    // Wrap aead client in mesos client
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    val validImpressionCount = 130
    val invalidImpressionCount = 70

    val impressions =
      MutableList(validImpressionCount) {
        LABELED_IMPRESSION_1.copy {
          vid = (it + 1).toLong()
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      }

    val invalidImpressions =
      List(invalidImpressionCount) {
        LABELED_IMPRESSION_2.copy {
          vid = (it + validImpressionCount + 1).toLong()
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      }

    impressions.addAll(invalidImpressions)

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET).toPath())
    val impressionsDekStorageClient =
      SelectedStorageClient(IMPRESSIONS_DEK_FILE_URI, dekTmpPath)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails =
      BlobDetails.newBuilder()
        .setBlobUri(IMPRESSIONS_FILE_URI)
        .setEncryptedDek(encryptedDek)
        .build()

    impressionsDekStorageClient.writeBlob(
      IMPRESSION_DEK_BLOB_KEY,
      blobDetails.toByteString()
    )

    // Create EventReader
    val eventReader = EventReader(
      kmsClient,
      StorageConfig(rootDirectory = impressionsTmpPath),
      StorageConfig(rootDirectory = dekTmpPath),
      IMPRESSIONS_DEK_FILE_URI_PREFIX
    )

    val result = RequisitionSpecs.getSampledVids(
      requisitionSpec,
      vidSamplingInterval,
      typeRegistry,
      eventReader
    )

    assertThat(result.count()).isEqualTo(validImpressionCount)
  }

  companion object {
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    private val PERSON_1 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val PERSON_2 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.FEMALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT_1 = testEvent { person = PERSON_1 }

    private val TEST_EVENT_2 = testEvent { person = PERSON_2 }


    private val LABELED_IMPRESSION_1 =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(
          TEST_EVENT_1.pack()
        )
        .build()

    private val LABELED_IMPRESSION_2 =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(
          TEST_EVENT_2.pack()
        )
        .build()

    private const val EVENT_GROUP_NAME = "dataProviders/someDataProvider/eventGroups/name"

    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_BLOB_KEY = "impressions"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET/$IMPRESSIONS_BLOB_KEY"

    private const val IMPRESSIONS_DEK_BUCKET = "impression-dek-bucket"
    private val IMPRESSION_DEK_BLOB_KEY =
      "ds/${TIME_RANGE.start}/event-group-id/$EVENT_GROUP_NAME/metadata"
    private val IMPRESSIONS_DEK_FILE_URI =
      "file:///$IMPRESSIONS_DEK_BUCKET/$IMPRESSION_DEK_BLOB_KEY"
    private const val IMPRESSIONS_DEK_FILE_URI_PREFIX = "file:///$IMPRESSIONS_DEK_BUCKET"
  }
}
