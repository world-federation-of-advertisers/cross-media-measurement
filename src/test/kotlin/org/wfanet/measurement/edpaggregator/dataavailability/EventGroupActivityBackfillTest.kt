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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class EventGroupActivityBackfillTest {
  private val registeredMetadata = mutableListOf<ImpressionMetadata>()
  private val listRequests = mutableListOf<ListImpressionMetadataRequest>()
  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listRequests += request
          val matching =
            registeredMetadata.filter {
              it.blobUri.startsWith(request.filter.blobUriPrefix) &&
                (request.showDeleted || it.state != ImpressionMetadata.State.DELETED) &&
                (request.filter.state == ImpressionMetadata.State.STATE_UNSPECIFIED ||
                  request.filter.state == it.state)
            }
          val pageStart = request.pageToken.ifEmpty { "0" }.toInt()
          val page = matching.drop(pageStart).take(request.pageSize)
          listImpressionMetadataResponse {
            impressionMetadata += page
            val nextPageStart = pageStart + page.size
            if (nextPageStart < matching.size) {
              nextPageToken = nextPageStart.toString()
            }
          }
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val throttler =
    object : Throttler {
      override suspend fun <T> onReady(block: suspend () -> T): T = block()
    }

  @Test
  fun `backfill publishes only active metadata and paginates list requests`() =
    runBlocking<Unit> {
      val storageClient = InMemoryStorageClient()
      storageClient.writeBlob("edp/meta/model-line/2/2026-01-02/done", ByteString.EMPTY)
      storageClient.writeBlob("edp/meta/model-line/1/2026-01-01/done", ByteString.EMPTY)
      storageClient.writeBlob("edp/other/model-line/1/2026-01-01/done", ByteString.EMPTY)
      registeredMetadata +=
        impressionMetadataRecord(
          name = "active-1",
          blobUri = "gs://bucket/edp/meta/model-line/1/2026-01-01/metadata-1.binpb",
          state = ImpressionMetadata.State.ACTIVE,
        )
      registeredMetadata +=
        impressionMetadataRecord(
          name = "deleted-1",
          blobUri = "gs://bucket/edp/meta/model-line/1/2026-01-01/metadata-2.binpb",
          state = ImpressionMetadata.State.DELETED,
        )
      registeredMetadata +=
        impressionMetadataRecord(
          name = "active-2",
          blobUri = "gs://bucket/edp/meta/model-line/1/2026-01-01/metadata-3.binpb",
          state = ImpressionMetadata.State.ACTIVE,
        )
      registeredMetadata +=
        impressionMetadataRecord(
          name = "deleted-2",
          blobUri = "gs://bucket/edp/meta/model-line/2/2026-01-02/metadata.binpb",
          state = ImpressionMetadata.State.DELETED,
        )
      val publishedMetadata = mutableListOf<List<ImpressionMetadata>>()
      val backfill =
        newBackfill(
          storageClient = storageClient,
          selection = EventGroupActivityBackfill.Selection.BlobPrefix(value = "edp/meta"),
          impressionMetadataPageSize = 1,
          publishEventGroupActivities = { metadata ->
            publishedMetadata += metadata.toList()
            metadata.size
          },
        )

      val result = backfill.backfill()

      assertThat(publishedMetadata.map { metadata -> metadata.map { it.name } })
        .containsExactly(listOf("active-1", "active-2"))
      assertThat(listRequests.map { it.filter.blobUriPrefix })
        .containsExactly(
          "gs://bucket/edp/meta/model-line/1/2026-01-01/",
          "gs://bucket/edp/meta/model-line/1/2026-01-01/",
          "gs://bucket/edp/meta/model-line/2/2026-01-02/",
        )
        .inOrder()
      assertThat(listRequests.map { it.filter.state }.distinct())
        .containsExactly(ImpressionMetadata.State.ACTIVE)
      assertThat(listRequests.map { it.showDeleted }.distinct()).containsExactly(false)
      assertThat(result.doneBlobsSelected).isEqualTo(2)
      assertThat(result.activityPairsPublished).isEqualTo(2)
      assertThat(result.noWork).isEqualTo(1)
      assertThat(result.errors).isEmpty()
    }

  @Test
  fun `backfill continues after publisher failure`() =
    runBlocking<Unit> {
      val storageClient = InMemoryStorageClient()
      for (date in 1..3) {
        storageClient.writeBlob("edp/meta/model-line/1/2026-01-0$date/done", ByteString.EMPTY)
        registeredMetadata +=
          impressionMetadataRecord(
            name = "metadata-$date",
            blobUri = "gs://bucket/edp/meta/model-line/1/2026-01-0$date/metadata.binpb",
            state = ImpressionMetadata.State.ACTIVE,
          )
      }
      val publishedNames = mutableListOf<String>()
      val rpcFailure = Status.PERMISSION_DENIED.asException()
      val failure = Exception("Error publishing EventGroup activities", rpcFailure)
      val backfill =
        newBackfill(
          storageClient = storageClient,
          selection = EventGroupActivityBackfill.Selection.BlobPrefix(value = "edp/meta"),
          impressionMetadataPageSize = 10,
          publishEventGroupActivities = { metadata ->
            val name = metadata.single().name
            if (name == "metadata-2") {
              throw failure
            }
            publishedNames += name
            1
          },
        )

      val result = backfill.backfill()

      assertThat(publishedNames).containsExactly("metadata-1", "metadata-3").inOrder()
      assertThat(result.doneBlobsSelected).isEqualTo(3)
      assertThat(result.activityPairsPublished).isEqualTo(2)
      assertThat(result.noWork).isEqualTo(0)
      assertThat(result.errors)
        .containsExactly(
          EventGroupActivityBackfill.BackfillError(
            doneBlobUri = "gs://bucket/edp/meta/model-line/1/2026-01-02/done",
            exception = failure,
          )
        )
      assertThat(result.errors.single().exception).isSameInstanceAs(failure)
      assertThat(result.errors.single().exception.cause).isSameInstanceAs(rpcFailure)
      assertThat(result.errors.single().message).contains("publishing EventGroup activities")
    }

  @Test
  fun `backfill returns no work when prefix has no completion blobs`() =
    runBlocking<Unit> {
      val storageClient = InMemoryStorageClient()
      storageClient.writeBlob(
        "edp/meta/model-line/1/2026-01-01/metadata.binpb",
        ByteString.copyFromUtf8("metadata"),
      )
      val backfill =
        newBackfill(
          storageClient = storageClient,
          selection = EventGroupActivityBackfill.Selection.BlobPrefix(value = "edp/meta"),
          impressionMetadataPageSize = 10,
          publishEventGroupActivities = { error("publisher must not be invoked") },
        )

      val result = backfill.backfill()

      assertThat(listRequests).isEmpty()
      assertThat(result.doneBlobsSelected).isEqualTo(0)
      assertThat(result.activityPairsPublished).isEqualTo(0)
      assertThat(result.noWork).isEqualTo(0)
      assertThat(result.errors).isEmpty()
    }

  @Test
  fun `backfill processes explicitly supplied completion blobs in order`() =
    runBlocking<Unit> {
      registeredMetadata +=
        impressionMetadataRecord(
          name = "metadata-2",
          blobUri = "gs://bucket/edp/meta/model-line/2/2026-01-02/metadata.binpb",
          state = ImpressionMetadata.State.ACTIVE,
        )
      registeredMetadata +=
        impressionMetadataRecord(
          name = "metadata-1",
          blobUri = "gs://bucket/edp/meta/model-line/1/2026-01-01/metadata.binpb",
          state = ImpressionMetadata.State.ACTIVE,
        )
      val publishedNames = mutableListOf<String>()
      val backfill =
        newBackfill(
          storageClient = InMemoryStorageClient(),
          selection =
            EventGroupActivityBackfill.Selection.DoneBlobUris(
              values =
                setOf(
                  "gs://bucket/edp/meta/model-line/2/2026-01-02/done",
                  "gs://bucket/edp/meta/model-line/1/2026-01-01/done",
                )
            ),
          impressionMetadataPageSize = 10,
          publishEventGroupActivities = { metadata ->
            publishedNames += metadata.single().name
            1
          },
        )

      val result = backfill.backfill()

      assertThat(publishedNames).containsExactly("metadata-1", "metadata-2").inOrder()
      assertThat(result.doneBlobsSelected).isEqualTo(2)
      assertThat(result.activityPairsPublished).isEqualTo(2)
      assertThat(result.noWork).isEqualTo(0)
      assertThat(result.errors).isEmpty()
    }

  private fun newBackfill(
    storageClient: StorageClient,
    selection: EventGroupActivityBackfill.Selection,
    impressionMetadataPageSize: Int,
    publishEventGroupActivities: suspend (Collection<ImpressionMetadata>) -> Int,
  ): EventGroupActivityBackfill =
    EventGroupActivityBackfill(
      storageClient = storageClient,
      storageRootUri = BlobUri(scheme = "gs", bucket = "bucket", key = ""),
      selection = selection,
      impressionMetadataStub = impressionMetadataStub,
      dataProviderName = DATA_PROVIDER_NAME,
      throttler = throttler,
      impressionMetadataPageSize = impressionMetadataPageSize,
      publishEventGroupActivities = publishEventGroupActivities,
    )

  private fun impressionMetadataRecord(
    name: String,
    blobUri: String,
    state: ImpressionMetadata.State,
  ): ImpressionMetadata = impressionMetadata {
    this.name = name
    this.blobUri = blobUri
    this.state = state
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/data-provider"
  }
}
