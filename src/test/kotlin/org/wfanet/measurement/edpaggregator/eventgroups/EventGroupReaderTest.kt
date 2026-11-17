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

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.gson.stream.MalformedJsonException
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroups
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class EventGroupReaderTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `read returns EventGroups from JSON with camel-case field name`() = runBlocking {
    writeJson("event-groups.json", EVENT_GROUPS, preserveProtoFieldNames = false)

    val actual = reader().read("event-groups.json").toList()

    assertEventGroups(actual)
  }

  @Test
  fun `read returns EventGroups from JSON with proto field name`() = runBlocking {
    writeJson("event-groups.json", EVENT_GROUPS, preserveProtoFieldNames = true)

    val actual = reader().read("event-groups.json").toList()

    assertEventGroups(actual)
  }

  @Test
  fun `read ignores unknown top-level JSON fields`() = runBlocking {
    val json =
      JsonFormat.printer()
        .print(eventGroups { eventGroups += EVENT_GROUPS })
        .replaceFirst("{", "{\"ignored\": {\"nested\": true},")
    storageClient().writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8(json)))

    val actual = reader().read("event-groups.json").toList()

    assertEventGroups(actual)
  }

  @Test
  fun `read rejects trailing JSON after EventGroups wrapper`(): Unit = runBlocking {
    val json = JsonFormat.printer().print(eventGroups { eventGroups += EVENT_GROUPS }) + "{}"
    storageClient().writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8(json)))

    assertFailsWith<MalformedJsonException> { reader().read("event-groups.json").toList() }
  }

  @Test
  fun `read rejects trailing garbage after EventGroups wrapper`(): Unit = runBlocking {
    val json = JsonFormat.printer().print(eventGroups { eventGroups += EVENT_GROUPS }) + "garbage"
    storageClient().writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8(json)))

    assertFailsWith<MalformedJsonException> { reader().read("event-groups.json").toList() }
  }

  @Test
  fun `read accepts trailing whitespace after EventGroups wrapper`() = runBlocking {
    val json = JsonFormat.printer().print(eventGroups { eventGroups += EVENT_GROUPS }) + " \n\t\r"
    storageClient().writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8(json)))

    val actual = reader().read("event-groups.json").toList()

    assertEventGroups(actual)
  }

  @Test
  fun `read returns EventGroups from Mesos RecordIO`() = runBlocking {
    writeBinpb("event-groups.binpb", EVENT_GROUPS)

    val actual = reader().read("event-groups.binpb").toList()

    assertEventGroups(actual)
  }

  @Test
  fun `read throws when JSON file is empty`() = runBlocking {
    writeJson("event-groups.json", emptyList(), preserveProtoFieldNames = false)

    val exception =
      assertFailsWith<IllegalStateException> { reader().read("event-groups.json").toList() }

    assertThat(exception).hasMessageThat().contains("zero event groups")
  }

  @Test
  fun `read throws when JSON file does not contain EventGroups`() = runBlocking {
    storageClient()
      .writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8("{\"anotherField\": []}")))

    val exception =
      assertFailsWith<IllegalStateException> { reader().read("event-groups.json").toList() }

    assertThat(exception).hasMessageThat().contains("zero event groups")
  }

  @Test
  fun `read throws when Mesos RecordIO file is empty`() = runBlocking {
    writeBinpb("event-groups.binpb", emptyList())

    val exception =
      assertFailsWith<IllegalStateException> { reader().read("event-groups.binpb").toList() }

    assertThat(exception).hasMessageThat().contains("zero event groups")
  }

  @Test
  fun `read throws when JSON record is not an EventGroup`(): Unit = runBlocking {
    storageClient()
      .writeBlob("event-groups.json", flowOf(ByteString.copyFromUtf8("{\"eventGroups\":[true]}")))

    assertFailsWith<InvalidProtocolBufferException> { reader().read("event-groups.json").toList() }
  }

  @Test
  fun `read throws when Mesos RecordIO record is not an EventGroup`(): Unit = runBlocking {
    storageClient().writeBlob("event-groups.binpb", flowOf(ByteString.copyFromUtf8("4\nbad!")))

    assertFailsWith<InvalidProtocolBufferException> { reader().read("event-groups.binpb").toList() }
  }

  @Test
  fun `read throws when JSON blob is missing`() = runBlocking {
    val exception =
      assertFailsWith<IllegalStateException> { reader().read("missing.json").toList() }

    assertThat(exception).hasMessageThat().contains("missing.json")
  }

  @Test
  fun `read throws when Mesos RecordIO blob is missing`() = runBlocking {
    val exception =
      assertFailsWith<IllegalStateException> { reader().read("missing.binpb").toList() }

    assertThat(exception).hasMessageThat().contains("missing.binpb")
  }

  @Test
  fun `read throws when extension is unsupported`() = runBlocking {
    val exception =
      assertFailsWith<IllegalStateException> { reader().read("event-groups.txt").toList() }

    assertThat(exception).hasMessageThat().contains("Unsupported EventGroup file format")
  }

  private fun reader(): EventGroupReader = EventGroupReader(storageClient())

  private fun storageClient(): FileSystemStorageClient =
    FileSystemStorageClient(temporaryFolder.root)

  private suspend fun writeJson(
    name: String,
    eventGroupMessages: List<EventGroup>,
    preserveProtoFieldNames: Boolean,
  ) {
    var printer = JsonFormat.printer()
    if (preserveProtoFieldNames) {
      printer = printer.preservingProtoFieldNames()
    }
    val json = printer.print(eventGroups { eventGroups += eventGroupMessages })
    storageClient().writeBlob(name, flowOf(ByteString.copyFromUtf8(json)))
  }

  private suspend fun writeBinpb(name: String, eventGroupMessages: List<EventGroup>) {
    MesosRecordIoStorageClient(storageClient())
      .writeBlob(name, eventGroupMessages.asFlow().map(EventGroup::toByteString))
  }

  private fun assertEventGroups(actual: List<EventGroup>) {
    assertThat(actual).hasSize(EVENT_GROUPS.size)
    actual.zip(EVENT_GROUPS).forEach { (actualEventGroup, expectedEventGroup) ->
      assertThat(actualEventGroup).isEqualTo(expectedEventGroup)
    }
  }

  private companion object {
    val EVENT_GROUPS: List<EventGroup> =
      listOf(
        eventGroup { eventGroupReferenceId = "campaign-1" },
        eventGroup { eventGroupReferenceId = "campaign-2" },
      )
  }
}
