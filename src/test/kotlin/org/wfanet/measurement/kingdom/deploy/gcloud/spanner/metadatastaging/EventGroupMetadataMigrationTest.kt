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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

import com.google.cloud.ByteArray
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Struct
import com.google.protobuf.struct
import com.google.protobuf.value
import java.nio.file.Files
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

@RunWith(JUnit4::class)
class EventGroupMetadataMigrationTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `migration preserves existing metadata and initializes its publication version`() =
    runBlocking<Unit> {
      val source = Schemata.KINGDOM_CHANGELOG_PATH
      val directory = temporaryFolder.newFolder("schema").toPath()
      Files.list(source.parent).use { files ->
        files.forEach { Files.copy(it, directory.resolve(it.fileName.toString())) }
      }
      val changelog = directory.resolve(source.fileName.toString())
      val completeChangelog = Files.readString(changelog)
      val migrationOffset =
        completeChangelog.indexOf("- include:\n    file: add-event-group-metadata-versions.sql")
      check(migrationOffset >= 0)
      Files.writeString(changelog, completeChangelog.substring(0, migrationOffset))
      val databaseId = "metadata-migration-test"
      val client = spannerEmulator.createDatabase(changelog, databaseId)
      try {
        val metadata = struct {
          fields["campaign_name"] = value { stringValue = "existing campaign" }
        }
        client.write(
          listOf(
            Mutation.newInsertBuilder("Certificates")
              .apply {
                set("CertificateId").to(1L)
                set("SubjectKeyIdentifier").to(ByteArray.copyFrom("migration-certificate"))
                set("NotValidBefore").to(Timestamp.parseTimestamp("2026-01-01T00:00:00Z"))
                set("NotValidAfter").to(Timestamp.parseTimestamp("2027-01-01T00:00:00Z"))
                set("RevocationState").to(0L)
              }
              .build(),
            Mutation.newInsertBuilder("MeasurementConsumers")
              .apply {
                set("MeasurementConsumerId").to(1L)
                set("ExternalMeasurementConsumerId").to(1L)
                set("PublicKeyCertificateId").to(1L)
                set("MeasurementConsumerDetails")
                  .to(Value.protoMessage(MeasurementConsumerDetails.getDefaultInstance()))
              }
              .build(),
            eventGroup(1L)
              .set("EntityMetadata")
              .to(Value.protoMessage(metadata))
              .set("EntityMetadataValues")
              .toStringArray(listOf("existing campaign"))
              .build(),
            eventGroup(2L).build(),
          )
        )
        Files.writeString(changelog, completeChangelog)
        spannerEmulator.createDatabase(changelog, databaseId)
        // Reapplying the changelog must not reset versions or alter existing metadata.
        spannerEmulator.createDatabase(changelog, databaseId)
        client.readOnlyTransaction().use { context ->
          for (id in listOf(1L, 2L)) {
            val row =
              requireNotNull(
                context.readRow(
                  "EventGroups",
                  Key.of(1L, id),
                  listOf(
                    "EntityMetadata",
                    "EntityMetadataValues",
                    "EntityMetadataPublicationSequence",
                  ),
                )
              )
            assertThat(row.getLong("EntityMetadataPublicationSequence")).isEqualTo(0L)
          }
          val row =
            requireNotNull(
              context.readRow(
                "EventGroups",
                Key.of(1L, 1L),
                listOf("EntityMetadata", "EntityMetadataValues"),
              )
            )
          assertThat(row.getProtoMessage("EntityMetadata", Struct.getDefaultInstance()))
            .isEqualTo(metadata)
          assertThat(row.getStringList("EntityMetadataValues")).containsExactly("existing campaign")
          val legacy =
            requireNotNull(
              context.readRow(
                "EventGroups",
                Key.of(1L, 2L),
                listOf("EntityMetadata", "EntityMetadataValues"),
              )
            )
          assertThat(legacy.isNull("EntityMetadata")).isTrue()
          assertThat(legacy.isNull("EntityMetadataValues")).isTrue()
          val reader = PublishedMetadataReader(context, 1, 1)
          assertThat(reader.get(1)).isEqualTo(metadata)
          assertThat(reader.search("existing")).containsExactly(1L, metadata)
        }
      } finally {
        spannerEmulator.deleteDatabase(databaseId)
      }
    }

  private fun eventGroup(id: Long): Mutation.WriteBuilder =
    Mutation.newInsertBuilder("EventGroups").apply {
      set("DataProviderId").to(1L)
      set("MeasurementConsumerId").to(1L)
      set("EventGroupId").to(id)
      set("ExternalEventGroupId").to(id)
      set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      set("State").to(EventGroup.State.ACTIVE.number.toLong())
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
