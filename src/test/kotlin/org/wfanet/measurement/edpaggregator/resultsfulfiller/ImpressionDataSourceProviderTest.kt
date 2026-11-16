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
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class ImpressionDataSourceProviderTest {

  @get:Rule val tmp = TemporaryFolder()
  val modelLine = "model-line-1"
  val fullModelLine = "modelProviders/provider/modelSuites/suite/modelLines/model-line-1"
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

  private fun createService(
    modelLineDataSources: Map<String, ModelLineDataSource> = emptyMap(),
    modelLineStorageClient: StorageClient? = null,
  ): ImpressionDataSourceProvider {
    val storageClientFactory: (BlobUri) -> StorageClient =
      modelLineStorageClient?.let { storageClient -> { _: BlobUri -> storageClient } }
        ?: { blobUri ->
          SelectedStorageClient(blobUri, tmp.root).underlyingClient
        }
    return ImpressionDataSourceProvider(
      impressionMetadataStub = impressionMetadataStub,
      dataProvider = "dataProviders/123",
      impressionsMetadataStorageConfig = StorageConfig(rootDirectory = tmp.root),
      modelLineDataSources = modelLineDataSources,
      modelLineStorageClientFactory = storageClientFactory,
    )
  }

  private suspend fun writeDoneMarker(
    storageClient: InMemoryStorageClient,
    blobKey: String,
    synced: Boolean,
  ) {
    storageClient.writeBlob(blobKey, ByteString.copyFromUtf8("done"))
    if (synced) {
      storageClient.updateBlobMetadata(
        blobKey,
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )
    }
  }

  private fun referenceEventGroup(referenceId: String, period: Interval): ResolvedEventGroup {
    return ResolvedEventGroup(
      collectionIntervals = listOf(period),
      eventGroupReferenceId = referenceId,
      entityKey = null,
    )
  }

  private fun entityEventGroup(
    entityType: String,
    entityId: String,
    period: Interval,
  ): ResolvedEventGroup {
    return ResolvedEventGroup(
      collectionIntervals = listOf(period),
      eventGroupReferenceId = null,
      entityKey =
        LabeledImpressionKt.entityKey {
          this.entityType = entityType
          this.entityId = entityId
        },
    )
  }

  @Test
  fun `selected model line fails when requested date is not finalized`(): Unit = runBlocking {
    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse {})
    File(tmp.root, "availability").mkdirs()
    val date = LocalDate.of(2026, 10, 5)
    val period = interval {
      startTime = date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      endTime = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
    }

    val exception =
      assertFailsWith<IllegalStateException> {
        createService(
            mapOf(
              fullModelLine to
                ModelLineDataSource(
                  "file:///availability/root",
                  validStartDate = date,
                  validEndDate = null,
                )
            )
          )
          .listImpressionDataSources(
            fullModelLine,
            listOf(referenceEventGroup("eg-missing", period)),
            reportInterval = period,
            requireModelLineFinalization = true,
          )
      }

    assertThat(exception).hasMessageThat().contains("not finalized")
  }

  @Test
  fun `selected model line accepts synced legacy and upload markers for zero impressions`(): Unit =
    runBlocking {
      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})
      val date = LocalDate.of(2026, 10, 5)
      val period = interval {
        startTime = date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
        endTime = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      }
      for (markerName in listOf("done", "done-${"a".repeat(64)}")) {
        val storageClient = InMemoryStorageClient()
        writeDoneMarker(
          storageClient,
          "root/model-line/model-line-1/$date/$markerName",
          synced = true,
        )

        val sources =
          createService(
              mapOf(
                fullModelLine to
                  ModelLineDataSource(
                    "file:///availability/root",
                    validStartDate = date,
                    validEndDate = null,
                  )
              ),
              storageClient,
            )
            .listImpressionDataSources(
              fullModelLine,
              listOf(referenceEventGroup("eg-zero", period)),
              reportInterval = period,
              requireModelLineFinalization = true,
            )

        assertThat(sources).isEmpty()
      }
    }

  @Test
  fun `selected model line rejects unprocessed completion markers`(): Unit = runBlocking {
    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse {})
    val date = LocalDate.of(2026, 10, 5)
    val period = interval {
      startTime = date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      endTime = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
    }
    for (markerName in listOf("done", "done-${"a".repeat(64)}")) {
      val storageClient = InMemoryStorageClient()
      writeDoneMarker(
        storageClient,
        "root/model-line/model-line-1/$date/$markerName",
        synced = false,
      )

      val exception =
        assertFailsWith<IllegalStateException> {
          createService(
              mapOf(
                fullModelLine to
                  ModelLineDataSource(
                    "file:///availability/root",
                    validStartDate = date,
                    validEndDate = null,
                  )
              ),
              storageClient,
            )
            .listImpressionDataSources(
              fullModelLine,
              listOf(referenceEventGroup("eg-unprocessed", period)),
              reportInterval = period,
              requireModelLineFinalization = true,
            )
        }

      assertThat(exception).hasMessageThat().contains("unprocessed completion marker")
    }
  }

  @Test
  fun `selected model line rejects date when any completion marker is unprocessed`(): Unit =
    runBlocking {
      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})
      val date = LocalDate.of(2026, 10, 5)
      val period = interval {
        startTime = date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
        endTime = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      }
      val storageClient = InMemoryStorageClient()
      writeDoneMarker(
        storageClient,
        "root/model-line/model-line-1/$date/done-${"a".repeat(64)}",
        synced = true,
      )
      writeDoneMarker(
        storageClient,
        "root/model-line/model-line-1/$date/done-${"b".repeat(64)}",
        synced = false,
      )

      val exception =
        assertFailsWith<IllegalStateException> {
          createService(
              mapOf(
                fullModelLine to
                  ModelLineDataSource(
                    "file:///availability/root",
                    validStartDate = date,
                    validEndDate = null,
                  )
              ),
              storageClient,
            )
            .listImpressionDataSources(
              fullModelLine,
              listOf(referenceEventGroup("eg-partial", period)),
              reportInterval = period,
              requireModelLineFinalization = true,
            )
        }

      assertThat(exception).hasMessageThat().contains("unprocessed completion marker")
    }

  @Test
  fun `cutover selection fails when finalization data source is missing`(): Unit = runBlocking {
    val date = LocalDate.of(2026, 10, 5)
    val reportInterval = interval {
      startTime = date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      endTime = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
    }

    val exception =
      assertFailsWith<IllegalStateException> {
        createService()
          .listImpressionDataSources(
            fullModelLine,
            listOf(referenceEventGroup("eg-missing-source", reportInterval)),
            reportInterval = reportInterval,
            requireModelLineFinalization = true,
          )
      }

    assertThat(exception).hasMessageThat().contains("Missing finalization data source")
  }

  @Test
  fun `cutover selection validates dates outside current collection intervals`(): Unit =
    runBlocking {
      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})
      val firstDate = LocalDate.of(2026, 10, 5)
      val collectionDate = firstDate.plusDays(1)
      val availabilityDir = File(tmp.root, "availability").also(File::mkdirs)
      FileSystemStorageClient(availabilityDir)
        .writeBlob(
          "root/model-line/model-line-1/$collectionDate/done",
          ByteString.copyFromUtf8("done"),
        )
      val collectionInterval = interval {
        startTime = collectionDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
        endTime =
          collectionDate.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      }
      val reportInterval = interval {
        startTime = firstDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
        endTime = firstDate.plusDays(2).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
      }

      val exception =
        assertFailsWith<IllegalStateException> {
          createService(
              mapOf(
                fullModelLine to
                  ModelLineDataSource(
                    "file:///availability/root",
                    validStartDate = firstDate,
                    validEndDate = null,
                  )
              )
            )
            .listImpressionDataSources(
              fullModelLine,
              listOf(referenceEventGroup("eg-narrow", collectionInterval)),
              reportInterval = reportInterval,
              requireModelLineFinalization = true,
            )
        }

      assertThat(exception).hasMessageThat().contains("not finalized on $firstDate")
    }

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
    val sources =
      svc.listImpressionDataSources(
        modelLine = modelLine,
        eventGroups = listOf(referenceEventGroup(eventGroupRef, period)),
      )

    assertThat(sources).hasSize(1)
    assertThat(sources[0].blobDetails.blobUri).isEqualTo("file:///impressions/$date/$eventGroupRef")
    assertThat(sources[0].interval.startTime).isEqualTo(start.toProtoTime())
    assertThat(sources[0].interval.endTime).isEqualTo(end.toProtoTime())
  }

  @Test
  fun `listImpressionDataSources prefers pinned output after metadata upgrade between queries`():
    Unit = runBlocking {
    val bucketName = "meta-bucket"
    val key = "metadata.binpb"
    val outputUri = "file:///impressions/shared"
    val bucketDir = File(tmp.root, bucketName).apply { mkdirs() }
    FileSystemStorageClient(bucketDir)
      .writeBlob(key, blobDetails { blobUri = outputUri }.toByteString())
    val period = interval {
      startTime = timestamp { seconds = 1 }
      endTime = timestamp { seconds = 2 }
    }
    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            blobUri = "file:///$bucketName/$key"
            interval = period
          }
        },
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            blobUri = "file:///$bucketName/$key"
            interval = period
            impressionsBlobGeneration = 7L
          }
        },
      )

    val sources =
      createService()
        .listImpressionDataSources(
          modelLine,
          listOf(
            ResolvedEventGroup(
              collectionIntervals = listOf(period),
              eventGroupReferenceId = "ref/123",
              entityKey =
                LabeledImpressionKt.entityKey {
                  entityType = "campaign"
                  entityId = "entity/123"
                },
            )
          ),
        )

    assertThat(sources).hasSize(1)
    assertThat(sources.single().blobDetails.blobUri).isEqualTo(outputUri)
    assertThat(sources.single().blobGeneration).isEqualTo(7L)
    val requests = argumentCaptor<ListImpressionMetadataRequest>()
    verify(impressionMetadataServiceMock, times(2)).listImpressionMetadata(requests.capture())
    assertThat(requests.firstValue.filter.entityKeysList).hasSize(1)
    assertThat(requests.secondValue.filter.eventGroupReferenceIdsList).containsExactly("ref/123")
  }

  @Test
  fun `storage-backed retains distinct output generations at the same metadata URI`(): Unit =
    runBlocking {
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
            for (generation in listOf(7L, 8L)) {
              impressionMetadata += impressionMetadata {
                state = ImpressionMetadata.State.ACTIVE
                impressionsBlobGeneration = generation
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
          }
        )

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
      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups = listOf(referenceEventGroup(eventGroupRef, period)),
        )

      assertThat(sources).hasSize(2)
      assertThat(sources.map { it.blobGeneration }).containsExactly(7L, 8L)
      assertThat(sources[0].blobDetails.blobUri)
        .isEqualTo("file:///impressions/$date/$eventGroupRef")
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

    val exception =
      assertFailsWith<ImpressionReadException> {
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups = listOf(referenceEventGroup("eg-3", period)),
        )
      }

    assertThat(exception.code).isEqualTo(ImpressionReadException.Code.BLOB_NOT_FOUND)
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
    val sources =
      svc.listImpressionDataSources(
        modelLine = modelLine,
        eventGroups = listOf(entityEventGroup("creative-id", "creative-a", period)),
      )

    assertThat(sources).hasSize(1)
    assertThat(sources[0].blobDetails.blobUri).isEqualTo("file:///impressions/$date/$sharedRefId")

    val captor = argumentCaptor<ListImpressionMetadataRequest>()
    verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
    val request = captor.firstValue
    assertThat(request.filter.entityKeysList).hasSize(1)
    assertThat(request.filter.entityKeysList[0].entityType).isEqualTo("creative-id")
    assertThat(request.filter.entityKeysList[0].entityId).isEqualTo("creative-a")
    assertThat(request.filter.eventGroupReferenceIdsList).isEmpty()
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
      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups = listOf(entityEventGroup("campaign-id", "campaign-123", period)),
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
      assertThat(request.filter.eventGroupReferenceIdsList).isEmpty()
    }

  @Test
  fun `storage-backed returns empty list when no metadata matches entity key`(): Unit =
    runBlocking {
      val svc = createService()

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse {})

      val date = LocalDate.of(2025, 3, 1)
      val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
      val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

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
      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups = listOf(entityEventGroup("creative-id", "nonexistent", period)),
        )

      assertThat(sources).isEmpty()
    }

  @Test
  fun `listImpressionDataSources by entity key builds correct filter`(): Unit = runBlocking {
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
        listOf(entityEventGroup("placement-id", "placement-42", period)),
      )

    assertThat(results).isEmpty()

    val captor = argumentCaptor<ListImpressionMetadataRequest>()
    verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
    val request = captor.firstValue
    assertThat(request.filter.entityKeysList).hasSize(1)
    assertThat(request.filter.entityKeysList[0].entityType).isEqualTo("placement-id")
    assertThat(request.filter.entityKeysList[0].entityId).isEqualTo("placement-42")
    assertThat(request.filter.eventGroupReferenceIdsList).isEmpty()
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
          listOf(
            referenceEventGroup("eg-1", period),
            referenceEventGroup("eg-2", period),
            referenceEventGroup("eg-3", period),
          ),
        )

      assertThat(results).isEmpty()

      val captor = argumentCaptor<ListImpressionMetadataRequest>()
      verify(impressionMetadataServiceMock).listImpressionMetadata(captor.capture())
      val request = captor.firstValue
      assertThat(request.filter.eventGroupReferenceIdsList).containsExactly("eg-1", "eg-2", "eg-3")
      assertThat(request.filter.entityKeysList).isEmpty()
      assertThat(request.filter.modelLine).isEqualTo(modelLine)
      assertThat(request.filter.hasIntervalOverlaps()).isTrue()
    }

  @Test
  fun `listImpressionDataSources retries a transient UNAVAILABLE from metadata service`(): Unit =
    runBlocking {
      val bucketName = "meta-bucket"
      val bucketDir = File(tmp.root, bucketName)
      bucketDir.mkdirs()

      val date = LocalDate.of(2025, 1, 15)
      val eventGroupRef = "eg-retry"
      val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$eventGroupRef/metadata"
      val blobDetailsBytes =
        blobDetails {
            blobUri = "file:///impressions/$date/$eventGroupRef"
            encryptedDek = EncryptedDek.getDefaultInstance()
          }
          .toByteString()
      FileSystemStorageClient(bucketDir).writeBlob(key, blobDetailsBytes)

      val start = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
      val end = date.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()
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

      // Fail transiently on the first two attempts, then succeed.
      val attempts = AtomicInteger(0)
      whenever(impressionMetadataServiceMock.listImpressionMetadata(any())).thenAnswer {
        if (attempts.incrementAndGet() < 3) {
          throw StatusException(Status.UNAVAILABLE)
        }
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$bucketName/$key"
            interval = period
          }
        }
      }

      val svc =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = tmp.root),
          listRetryBackoff =
            ExponentialBackoff(
              initialDelay = Duration.ofMillis(1),
              multiplier = 1.0,
              randomnessFactor = 0.0,
            ),
        )

      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups = listOf(referenceEventGroup(eventGroupRef, period)),
        )

      assertThat(sources).hasSize(1)
      assertThat(attempts.get()).isEqualTo(3)
    }

  @Test
  fun `listImpressionDataSources deduplicates metadata URIs before reading blob details`(): Unit =
    runBlocking {
      val svc = createService()
      val bucketName = "meta-bucket"
      val bucketDir = File(tmp.root, bucketName)
      bucketDir.mkdirs()
      val key = "duplicate/metadata"
      FileSystemStorageClient(bucketDir)
        .writeBlob(
          key,
          blobDetails {
              blobUri = "file:///impressions/duplicate"
              encryptedDek = EncryptedDek.getDefaultInstance()
            }
            .toByteString(),
        )
      val firstMetadata = impressionMetadata {
        blobUri = "file:///$bucketName/$key"
        interval = interval {
          startTime = timestamp { seconds = 1 }
          endTime = timestamp { seconds = 2 }
        }
      }
      val duplicateMetadata = impressionMetadata {
        blobUri = firstMetadata.blobUri
        interval = interval {
          startTime = timestamp { seconds = 3 }
          endTime = timestamp { seconds = 4 }
        }
      }

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(
          listImpressionMetadataResponse { impressionMetadata += firstMetadata },
          listImpressionMetadataResponse { impressionMetadata += duplicateMetadata },
        )

      val sources =
        svc.listImpressionDataSources(
          modelLine = modelLine,
          eventGroups =
            listOf(
              ResolvedEventGroup(
                collectionIntervals = listOf(firstMetadata.interval),
                eventGroupReferenceId = "ref/123",
                entityKey =
                  LabeledImpressionKt.entityKey {
                    entityType = "campaign"
                    entityId = "entity/123"
                  },
              )
            ),
        )

      assertThat(sources).hasSize(1)
      assertThat(sources.single().blobDetails.blobUri).isEqualTo("file:///impressions/duplicate")
      assertThat(sources.single().interval).isEqualTo(firstMetadata.interval)
      verify(impressionMetadataServiceMock, times(2)).listImpressionMetadata(any())
    }
}
