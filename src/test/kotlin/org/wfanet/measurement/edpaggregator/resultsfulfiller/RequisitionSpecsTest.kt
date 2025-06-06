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
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneId
import kotlin.streams.asSequence
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
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

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

    // Set up KMS
    val kekUri = FakeKmsClient.KEY_URI_PREFIX
    val kmsClient = EncryptedMesosStorage.createKmsClient(FakeKmsClient.KEY_URI_PREFIX)

    // Set up streaming encryption key
    val serializedEncryptionKey = EncryptedMesosStorage.generateSerializedEnryptionKey(kmsClient, kekUri)

    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    val mesosRecordIoStorageClient =
      EncryptedMesosStorage.createEncryptedMesosStorage(
        impressionsStorageClient,
        kmsClient,
        kekUri,
        serializedEncryptionKey
      )

    // Upload impressions for 2 days
    val validImpressionCount1 = 130
    val invalidImpressionCount1 = 70
    EncryptedMesosStorage.uploadImpressions(
      mesosRecordIoStorageClient,
      DS_1.toString(),
      validImpressionCount1,
      invalidImpressionCount1,
      LABELED_IMPRESSION_1,
      LABELED_IMPRESSION_2,
      TIME_RANGE.start.toProtoTime()
    )

    val validImpressionCount2 = 50
    val invalidImpressionCount2 = 200
    EncryptedMesosStorage.uploadImpressions(
      mesosRecordIoStorageClient,
      DS_2.toString(),
      validImpressionCount2,
      invalidImpressionCount2,
      LABELED_IMPRESSION_1,
      LABELED_IMPRESSION_2,
      TIME_RANGE.start.toProtoTime()
    )

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val dates = DS_1.datesUntil(DS_2.plusDays(1)).asSequence()
    dates.forEach { date ->
      EncryptedMesosStorage.uploadDek(
        impressionsDekStorageClient,
        kekUri, serializedEncryptionKey,
        "$IMPRESSIONS_FILE_URI/$date",
        "ds/$date/event-group-id/$EVENT_GROUP_NAME/metadata"
      )
    }

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

    assertThat(result.count()).isEqualTo(validImpressionCount1 + validImpressionCount2)
  }

  companion object {
    private val ZONE_ID =  ZoneId.of("America/New_York")
    private val LAST_EVENT_DATE = LocalDate.now(ZONE_ID)
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(0) // Subtracts 1 day
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    private val DS_1 = LocalDate.ofInstant(TIME_RANGE.start, ZONE_ID)
    private val DS_2 = LocalDate.ofInstant(TIME_RANGE.endExclusive, ZONE_ID)


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
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET"

    private const val IMPRESSIONS_DEK_BUCKET = "impression-dek-bucket"
    private const val IMPRESSIONS_DEK_FILE_URI_PREFIX = "file:///$IMPRESSIONS_DEK_BUCKET"
  }
}
