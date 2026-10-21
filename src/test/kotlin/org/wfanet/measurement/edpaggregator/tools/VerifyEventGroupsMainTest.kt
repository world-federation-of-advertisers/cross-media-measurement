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

package org.wfanet.measurement.edpaggregator.tools

import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import java.io.File
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.copy
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroups
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class VerifyEventGroupsMainTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `main prints help without requiring a file`() {
    val capturedOutput = CommandLineTesting.capturingOutput(arrayOf("--help"), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(0)
    subject.out().contains("--event-groups-file=<FILE>")
    subject.out().contains(".json")
    subject.out().contains(".binpb")
  }

  @Test
  fun `main validates JSON file`() {
    val file = writeJson("event-groups.json", listOf(VALID_EVENT_GROUP))

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(0)
    subject.out().contains("PASS:")
    subject.out().contains("verified 1 EventGroup")
  }

  @Test
  fun `main validates multiple files`() {
    val jsonFile = writeJson("event-groups.json", listOf(VALID_EVENT_GROUP))
    val binaryFile = writeBinpb("event-groups.binpb", listOf(VALID_EVENT_GROUP))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        arrayOf(
          "--event-groups-file",
          jsonFile.absolutePath,
          "--event-groups-file",
          binaryFile.absolutePath,
        ),
        ::main,
      )

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(0)
    subject.out().contains(jsonFile.path)
    subject.out().contains(binaryFile.path)
  }

  @Test
  fun `main rejects equal interval endpoints`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 200 }
        }
      }
    val file = writeJson("event-groups.json", listOf(eventGroup))

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("start_time must be before end_time")
  }

  @Test
  fun `main validates Mesos RecordIO binary protobuf file`() {
    val file = writeBinpb("event-groups.binpb", listOf(VALID_EVENT_GROUP))

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(0)
    subject.out().contains("verified 1 EventGroup")
  }

  @Test
  fun `main rejects bare measurement consumer ID in binary file`() {
    val eventGroup = VALID_EVENT_GROUP.copy { measurementConsumer = "mc-1" }
    val file = writeBinpb("event-groups.binpb", listOf(eventGroup))

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("expected measurementConsumers/<measurement-consumer-id>")
  }

  @Test
  fun `main reports every invalid EventGroup`() {
    val equalInterval =
      VALID_EVENT_GROUP.copy {
        entityKey = entityKey {
          entityType = "campaign"
          entityId = "campaign-2"
        }
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 200 }
        }
      }
    val bareMeasurementConsumer =
      VALID_EVENT_GROUP.copy {
        entityKey = entityKey {
          entityType = "campaign"
          entityId = "campaign-3"
        }
        measurementConsumer = "mc-1"
      }
    val file =
      writeJson(
        "event-groups.json",
        listOf(equalInterval, VALID_EVENT_GROUP, bareMeasurementConsumer),
      )

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("2 of 3 EventGroups are invalid")
  }

  @Test
  fun `main rejects empty file`() {
    val file = writeJson("event-groups.json", emptyList())

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("zero event groups")
  }

  @Test
  fun `main rejects empty binary file`() {
    val file = writeBinpb("event-groups.binpb", emptyList())

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("zero event groups")
  }

  @Test
  fun `main rejects malformed file`() {
    val file = temporaryFolder.newFile("event-groups.json")
    file.writeText("{ not-json")

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("parse failure after 0 successfully parsed EventGroups")
  }

  @Test
  fun `main rejects malformed Mesos RecordIO file`() {
    val file = temporaryFolder.newFile("event-groups.binpb")
    file.writeText("4\nbad!")

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("parse failure after 0 successfully parsed EventGroups")
  }

  @Test
  fun `main reports number of EventGroups parsed before JSON parse failure`() {
    val file = temporaryFolder.newFile("event-groups.json")
    val validEventGroupJson = JsonFormat.printer().print(VALID_EVENT_GROUP)
    file.writeText("""{"eventGroups":[$validEventGroupJson,true]}""")

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("parse failure after 1 successfully parsed EventGroup")
  }

  @Test
  fun `main reports number of EventGroups parsed before RecordIO parse failure`() {
    val file = temporaryFolder.newFile("event-groups.binpb")
    val validEventGroupBytes = VALID_EVENT_GROUP.toByteArray()
    file.writeBytes(
      "${validEventGroupBytes.size}\n".toByteArray() +
        validEventGroupBytes +
        "4\nbad!".toByteArray()
    )

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("parse failure after 1 successfully parsed EventGroup")
  }

  @Test
  fun `main rejects missing file`() {
    val file = File(temporaryFolder.root, "missing.json")

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("not a regular file")
  }

  @Test
  fun `main rejects unsupported extension`() {
    val file = temporaryFolder.newFile("event-groups.txt")
    file.writeText("{}")

    val capturedOutput =
      CommandLineTesting.capturingOutput(arrayOf("--event-groups-file", file.absolutePath), ::main)

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains("Unsupported EventGroup file format")
  }

  @Test
  fun `main continues with later files after a file fails`() {
    val invalidFile = temporaryFolder.newFile("invalid.json")
    invalidFile.writeText("{ not-json")
    val validFile = writeBinpb("valid.binpb", listOf(VALID_EVENT_GROUP))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        arrayOf(
          "--event-groups-file",
          invalidFile.absolutePath,
          "--event-groups-file",
          validFile.absolutePath,
        ),
        ::main,
      )

    val subject = CommandLineTesting.assertThat(capturedOutput)
    subject.status().isEqualTo(1)
    subject.err().contains(invalidFile.path)
    subject.out().contains("PASS: ${validFile.path}")
  }

  private fun writeJson(name: String, eventGroupMessages: List<EventGroup>): File {
    val file = temporaryFolder.newFile(name)
    val message = eventGroups { eventGroups += eventGroupMessages }
    file.writeText(JsonFormat.printer().print(message))
    return file
  }

  private fun writeBinpb(name: String, eventGroupMessages: List<EventGroup>): File {
    val file = File(temporaryFolder.root, name)
    runBlocking {
      MesosRecordIoStorageClient(FileSystemStorageClient(temporaryFolder.root))
        .writeBlob(name, eventGroupMessages.asFlow().map(EventGroup::toByteString))
    }
    return file
  }

  // CommandLineTesting uses this security manager to observe the real main function's exit code.
  @Suppress("DEPRECATION")
  companion object {
    private val VALID_EVENT_GROUP = eventGroup {
      eventGroupReferenceId = "campaign-1"
      eventGroupMetadata = metadata {
        adMetadata = adMetadata {
          campaignMetadata = campaignMetadata {
            brand = "Example brand"
            campaign = "Example campaign"
          }
        }
      }
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      measurementConsumer = "measurementConsumers/mc-1"
      mediaTypes += EventGroup.MediaType.VIDEO
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }

    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}
