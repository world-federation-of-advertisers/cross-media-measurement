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
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class ImpressionDataSourceProviderTest {

  @get:Rule val tmp = TemporaryFolder()
  val modelLine = "model-line-1"
  val impressionsBlobDetailsUriPrefix = "file:///meta-bucket/"

  private val impressionMetadataServiceMock =
    mockService<ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase>()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private val impressionMetadataStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private fun createService() =
    ImpressionDataSourceProvider(
      impressionMetadataStub = impressionMetadataStub,
      dataProvider = "dataProviders/123",
      impressionsMetadataStorageConfig = StorageConfig(rootDirectory = tmp.root),
    )

  @Test
  fun `storage-backed returns one source for a single day`(): Unit = runBlocking {
    val svc = createService()
    val bucketName = "meta-bucket"
    val bucketDir = File(tmp.root, bucketName)
    bucketDir.mkdirs()

    // Write BlobDetails at the resolved metadata path key within the bucket.
    val date = LocalDate.of(2025, 1, 15)
    val eventGroupRef = "eg-1"
    val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$eventGroupRef/metadata"
    val blobDetailsBytes =
      blobDetails {
          blobUri = "file:///impressions/$date/$eventGroupRef"
          encryptedDek = EncryptedDek.getDefaultInstance()
        }
        .toByteString()
    val fs = FileSystemStorageClient(bucketDir)
    fs.writeBlob(key, blobDetailsBytes)

    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$bucketName/$key"
            interval = interval {
              startTime = timestamp {
                seconds = start.epochSecond
                nanos = start.nano
              }
              endTime = timestamp {
                seconds = end.epochSecond
                nanos = end.nano
              }
            }
          }
        }
      )

    val sources =
      svc.listImpressionDataSources(
        modelLine = modelLine,
        selector = ImpressionQuerySelector.ByEventGroupReferenceId(eventGroupRef),
        period =
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
    val svc = createService()
    val rootDir = tmp.root
    val bucketName = "meta-bucket"
    File(rootDir, bucketName).mkdirs()

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$bucketName/metadata"
          }
        }
      )

    val date = LocalDate.of(2025, 2, 1)
    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

    try {
      svc.listImpressionDataSources(
        modelLine = modelLine,
        selector = ImpressionQuerySelector.ByEventGroupReferenceId("eg-3"),
        period =
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

  @Test
  fun `storage-backed returns one source for entity key query`(): Unit = runBlocking {
    val svc = createService()
    val bucketName = "meta-bucket"
    val bucketDir = File(tmp.root, bucketName)
    bucketDir.mkdirs()

    val date = LocalDate.of(2025, 1, 15)
    val sharedRefId = "multi-creative"
    val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$sharedRefId/metadata"
    val blobDetailsBytes =
      blobDetails {
          blobUri = "file:///impressions/$date/$sharedRefId"
          encryptedDek = EncryptedDek.getDefaultInstance()
        }
        .toByteString()
    val fs = FileSystemStorageClient(bucketDir)
    fs.writeBlob(key, blobDetailsBytes)

    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

    val queryEntityKey =
      org.wfanet.measurement.edpaggregator.v1alpha.entityKey {
        entityType = "creative-id"
        entityId = "creative-a"
      }

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$bucketName/$key"
            eventGroupReferenceId = sharedRefId
            interval = interval {
              startTime = timestamp {
                seconds = start.epochSecond
                nanos = start.nano
              }
              endTime = timestamp {
                seconds = end.epochSecond
                nanos = end.nano
              }
            }
          }
        }
      )

    val sources =
      svc.listImpressionDataSources(
        modelLine = modelLine,
        selector = ImpressionQuerySelector.ByEntityKey(queryEntityKey),
        period =
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
    assertThat(sources[0].blobDetails.blobUri).isEqualTo("file:///impressions/$date/$sharedRefId")

    val captor = argumentCaptor<ListImpressionMetadataRequest>()
    verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
    val request = captor.firstValue
    assertThat(request.filter.entityKeysList).hasSize(1)
    assertThat(request.filter.entityKeysList[0].entityType).isEqualTo("creative-id")
    assertThat(request.filter.entityKeysList[0].entityId).isEqualTo("creative-a")
    assertThat(request.filter.eventGroupReferenceId).isEmpty()
  }

  @Test
  fun `storage-backed returns multiple sources for multi-day entity key query`(): Unit =
    runBlocking {
      val svc = createService()
      val bucketName = "meta-bucket"
      val bucketDir = File(tmp.root, bucketName)
      bucketDir.mkdirs()

      val dates =
        listOf(LocalDate.of(2025, 1, 15), LocalDate.of(2025, 1, 16), LocalDate.of(2025, 1, 17))
      val sharedRefIds = listOf("ref-a", "ref-b", "ref-c")
      val fs = FileSystemStorageClient(bucketDir)

      for ((i, date) in dates.withIndex()) {
        val refId = sharedRefIds[i]
        val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$refId/metadata"
        val blobDetailsBytes =
          blobDetails {
              blobUri = "file:///impressions/$date/$refId"
              encryptedDek = EncryptedDek.getDefaultInstance()
            }
            .toByteString()
        fs.writeBlob(key, blobDetailsBytes)
      }

      val start = dates.first().atStartOfDay(ZoneId.of("UTC")).toInstant()
      val end = dates.last().plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

      val testModelLine = modelLine
      val metadataEntries =
        dates.mapIndexed { i, date ->
          val refId = sharedRefIds[i]
          val dayStart = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
          val dayEnd = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()
          impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri =
              "file:///$bucketName/ds/$date/model-line/$testModelLine/event-group-reference-id/$refId/metadata"
            eventGroupReferenceId = refId
            interval = interval {
              startTime = timestamp {
                seconds = dayStart.epochSecond
                nanos = dayStart.nano
              }
              endTime = timestamp {
                seconds = dayEnd.epochSecond
                nanos = dayEnd.nano
              }
            }
          }
        }

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += metadataEntries })

      val queryEntityKey =
        org.wfanet.measurement.edpaggregator.v1alpha.entityKey {
          entityType = "campaign-id"
          entityId = "campaign-123"
        }

      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          selector = ImpressionQuerySelector.ByEntityKey(queryEntityKey),
          period =
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

      assertThat(sources).hasSize(3)
      for ((i, source) in sources.withIndex()) {
        assertThat(source.blobDetails.blobUri)
          .isEqualTo("file:///impressions/${dates[i]}/${sharedRefIds[i]}")
      }

      val captor = argumentCaptor<ListImpressionMetadataRequest>()
      verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
      val request = captor.firstValue
      assertThat(request.filter.entityKeysList).hasSize(1)
      assertThat(request.filter.entityKeysList[0].entityType).isEqualTo("campaign-id")
      assertThat(request.filter.entityKeysList[0].entityId).isEqualTo("campaign-123")
      assertThat(request.filter.eventGroupReferenceId).isEmpty()
    }

  @Test
  fun `storage-backed returns empty list when no metadata matches entity key`(): Unit =
    runBlocking {
      val svc = createService()

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})

      val queryEntityKey =
        org.wfanet.measurement.edpaggregator.v1alpha.entityKey {
          entityType = "creative-id"
          entityId = "nonexistent"
        }

      val date = LocalDate.of(2025, 3, 1)
      val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
      val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          selector = ImpressionQuerySelector.ByEntityKey(queryEntityKey),
          period =
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

      assertThat(sources).isEmpty()
    }

  @Test
  fun `listImpressionDataSources by entity key builds correct filter`(): Unit = runBlocking {
    val svc = createService()

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse {})

    val queryEntityKey =
      org.wfanet.measurement.edpaggregator.v1alpha.entityKey {
        entityType = "placement-id"
        entityId = "placement-42"
      }

    val date = LocalDate.of(2025, 4, 10)
    val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = date.plusDays(5).atStartOfDay(ZoneId.of("UTC")).toInstant()

    val period = interval {
      startTime = timestamp {
        seconds = start.epochSecond
        nanos = start.nano
      }
      endTime = timestamp {
        seconds = end.epochSecond
        nanos = end.nano
      }
    }

    val results =
      svc.listImpressionDataSources(
        modelLine,
        ImpressionQuerySelector.ByEntityKey(queryEntityKey),
        period,
      )

    assertThat(results).isEmpty()

    val captor = argumentCaptor<ListImpressionMetadataRequest>()
    verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
    val request = captor.firstValue
    assertThat(request.filter.entityKeysList).hasSize(1)
    assertThat(request.filter.entityKeysList[0].entityType).isEqualTo("placement-id")
    assertThat(request.filter.entityKeysList[0].entityId).isEqualTo("placement-42")
    assertThat(request.filter.eventGroupReferenceId).isEmpty()
    assertThat(request.filter.modelLine).isEqualTo(modelLine)
    assertThat(request.filter.hasIntervalOverlaps()).isTrue()
  }

  @Test
  fun `listImpressionDataSources by event group reference ids builds correct filter`(): Unit =
    runBlocking {
      val svc = createService()

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})

      val date = LocalDate.of(2025, 4, 10)
      val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
      val end = date.plusDays(5).atStartOfDay(ZoneId.of("UTC")).toInstant()
      val period = interval {
        startTime = timestamp {
          seconds = start.epochSecond
          nanos = start.nano
        }
        endTime = timestamp {
          seconds = end.epochSecond
          nanos = end.nano
        }
      }

      val results =
        svc.listImpressionDataSources(
          modelLine,
          ImpressionQuerySelector.ByEventGroupReferenceIds(listOf("eg-1", "eg-2", "eg-3")),
          period,
        )

      assertThat(results).isEmpty()

      val captor = argumentCaptor<ListImpressionMetadataRequest>()
      verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
      val request = captor.firstValue
      assertThat(request.filter.eventGroupReferenceIdsList).containsExactly("eg-1", "eg-2", "eg-3")
      assertThat(request.filter.eventGroupReferenceId).isEmpty()
      assertThat(request.filter.entityKeysList).isEmpty()
      assertThat(request.filter.modelLine).isEqualTo(modelLine)
      assertThat(request.filter.hasIntervalOverlaps()).isTrue()
    }
}
