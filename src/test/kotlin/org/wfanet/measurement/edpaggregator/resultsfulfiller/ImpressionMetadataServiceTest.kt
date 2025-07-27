/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.protobuf.timestamp
import com.google.type.interval
import java.io.File
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class ImpressionMetadataServiceTest {

  @get:Rule val tmp = TemporaryFolder()

  @Test
  fun `storage-backed returns one source for a single day`(): Unit = runBlocking {
    val rootDir = tmp.root
    val bucketName = "meta-bucket"
    val bucketDir = File(rootDir, bucketName)
    bucketDir.mkdirs()

    val pathResolver =
      object : EventPathResolver {
        override suspend fun resolvePaths(
          date: LocalDate,
          eventGroupReferenceId: String,
        ): EventPathResolver.EventPaths {
          return EventPathResolver.EventPaths(
            metadataPath =
              "file:///$bucketName/ds/$date/event-group-reference-id/$eventGroupReferenceId/metadata",
            eventGroupReferenceId = eventGroupReferenceId,
          )
        }
      }

    // Write BlobDetails at the resolved metadata path key within the bucket.
    val date = LocalDate.of(2025, 1, 15)
    val eventGroupRef = "eg-1"
    val key = "ds/$date/event-group-reference-id/$eventGroupRef/metadata"
    val blobDetailsBytes =
      blobDetails {
          blobUri = "file:///impressions/$date/$eventGroupRef"
          encryptedDek = EncryptedDek.getDefaultInstance()
        }
        .toByteString()
    val fs = FileSystemStorageClient(bucketDir)
    fs.writeBlob(key, blobDetailsBytes)

    val svc =
      StorageImpressionMetadataService(
        pathResolver = pathResolver,
        impressionDekStorageConfig = StorageConfig(rootDirectory = rootDir),
        zoneIdForDates = ZoneId.of("UTC"),
      )

    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()
    val sources =
      svc.listImpressionDataSources(
        eventGroupRef,
        interval {
          startTime = timestamp {
            seconds = start.epochSecond
            nanos = start.nano
          }
          endTime = timestamp {
            seconds = end.epochSecond
            nanos = end.nano
          }
        },
      )

    assertThat(sources).hasSize(1)
    assertThat(sources[0].blobDetails.blobUri).isEqualTo("file:///impressions/$date/$eventGroupRef")
    assertThat(sources[0].interval.startTime).isEqualTo(start.toProtoTime())
    assertThat(sources[0].interval.endTime).isEqualTo(end.toProtoTime())
  }


  @Test
  fun `storage-backed throws when metadata is missing`(): Unit = runBlocking {
    val rootDir = tmp.root
    val bucketName = "meta-bucket"
    File(rootDir, bucketName).mkdirs()

    val pathResolver =
      object : EventPathResolver {
        override suspend fun resolvePaths(
          date: LocalDate,
          eventGroupReferenceId: String,
        ): EventPathResolver.EventPaths {
          return EventPathResolver.EventPaths(
            metadataPath =
              "file:///$bucketName/ds/$date/event-group-reference-id/$eventGroupReferenceId/metadata",
            eventGroupReferenceId = eventGroupReferenceId,
          )
        }
      }

    val svc =
      StorageImpressionMetadataService(
        pathResolver = pathResolver,
        impressionDekStorageConfig = StorageConfig(rootDirectory = rootDir),
        zoneIdForDates = ZoneId.of("UTC"),
      )

    val date = LocalDate.of(2025, 2, 1)
    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

    try {
      svc.listImpressionDataSources(
        "eg-3",
        interval {
          startTime = timestamp {
            seconds = start.epochSecond
            nanos = start.nano
          }
          endTime = timestamp {
            seconds = end.epochSecond
            nanos = end.nano
          }
        },
      )
      assert(false) { "Expected ImpressionReadException" }
    } catch (e: ImpressionReadException) {
      assertThat(e.code).isEqualTo(ImpressionReadException.Code.BLOB_NOT_FOUND)
    }
  }
}
