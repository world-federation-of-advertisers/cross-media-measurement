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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.type.interval
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.Rule
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import com.google.protobuf.timestamp
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import org.junit.rules.TemporaryFolder
import org.mockito.kotlin.any
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import java.time.Clock
import java.time.Duration
import org.mockito.kotlin.times
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.StorageClient
import java.io.File
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.emptyFlow

enum class BlobEncoding { PROTO, JSON }

@RunWith(JUnit4::class)
class DataAvailabilitySyncTest {

    private val bucket = "file:///my-bucket"
    private val folderPrefix = "edp/edp_name/"

    private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
        onBlocking { replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>()) }
            .thenAnswer {
                DataProvider.getDefaultInstance()
            }
    }

     private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
        onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
            .thenAnswer { invocation ->
                val request = invocation.getArgument<ListImpressionMetadataRequest>(0)

                // Build some fake proto data for the response
                listImpressionMetadataResponse {
                    impressionMetadata += listOf(
                        impressionMetadata {
                            name = "${request.parent}/impressionMetadata1"
                            modelLine = request.filter.modelLine
                            blobUri = "gs://bucket/blob-1"
                            interval = interval {
                                startTime = timestamp { seconds = 100 }
                                endTime = timestamp { seconds = 200 }
                            }
                            state = ImpressionMetadata.State.ACTIVE
                        },
                        impressionMetadata {
                            name = "${request.parent}/impressionMetadata2"
                            modelLine = request.filter.modelLine
                            blobUri = "gs://bucket/blob-2"
                            interval = interval {
                                startTime = timestamp { seconds = 200 }
                                endTime = timestamp { seconds = 300 }
                            }
                            state = ImpressionMetadata.State.ACTIVE
                        }
                    )

                }
            }
      }

    private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
        DataProvidersCoroutineStub(grpcTestServerRule.channel)
    }

    private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
        ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
    }

    @get:Rule val grpcTestServerRule = GrpcTestServerRule {
        addService(dataProvidersServiceMock)
        addService(impressionMetadataServiceMock)
    }

    @get:Rule val tempFolder = TemporaryFolder()

    @Test
    fun `register single contiguous day for existing model line using proto message`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(300L to 400L)
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `register single contiguous day for existing model line using json message`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(300L to 400L),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `register single overlapping day for existing model line`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(250L to 400L),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `registers a single contiguous day preceding an existing interval for an existing model line`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(50L to 100L),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `register multiple contiguous day for existing model line`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(
                    300L to 400L,
                    400L to 500L,
                    500L to 600L
                ),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `blob details with missing interval is ignored`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(null to null),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `invalid single interval fails for existing model line`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(320L to 400L),
                BlobEncoding.JSON
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `metadata file is ignored if file prefix doesn't follow expected path`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(300L to 400L)
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/some-wrong-path/done") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `metadata file is ignored if no associated impression file is found`() {

        val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

        runBlocking {
            seedBlobDetails(
                storageClient,
                folderPrefix,
                listOf(300L to 400L),
                createImpressionFile = false
            )
        }

        val dataAvailabilitySync = DataAvailabilitySync(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailabilitySync.sync("$bucket/${folderPrefix}done") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { listImpressionMetadata(any()) }
    }

    /**
     * Seeds a directory (prefix) with BlobDetails files, one per interval.
     * Returns the blob keys written.
     */
    suspend fun seedBlobDetails(
        storageClient: StorageClient,
        prefix: String,
        intervals: List<Pair<Long?, Long?>>,
        encoding: BlobEncoding = BlobEncoding.PROTO,
        createImpressionFile: Boolean = true,
    ): List<String> {
        require(prefix.isEmpty() || prefix.endsWith("/")) { "prefix should end with '/'" }

        val written = mutableListOf<String>()

        intervals.forEachIndexed { index, (startSeconds, endSeconds) ->
            val blobUri = "some_blob_uri_$index"
            val details = blobDetails {
                this.blobUri = blobUri
                eventGroupReferenceId = "event${index + 1}"
                modelLine = "modelLine1"
                interval = interval {
                    if (startSeconds != null) {
                        startTime = timestamp { seconds = startSeconds; nanos = 0 }
                    }
                    if (endSeconds != null) {
                        endTime = timestamp { seconds = endSeconds; nanos = 0 }
                    }
                }
            }

            val filename = when (encoding) {
                BlobEncoding.PROTO -> "metadata-$index.pb"
                BlobEncoding.JSON  -> "metadata-$index.json"
            }
            val key = "$prefix$filename"


            val bytes = details.serialize(encoding)
            storageClient.writeBlob(key, bytes)
            if(createImpressionFile) {
                storageClient.writeBlob(blobUri, emptyFlow())
            }

            written += key
        }

        return written
    }

    private fun BlobDetails.serialize(encoding: BlobEncoding): ByteString =
        when (encoding) {
            BlobEncoding.PROTO -> ByteString.copyFrom(this.toByteArray())
            BlobEncoding.JSON  -> ByteString.copyFromUtf8(JsonFormat.printer().print(this))
        }

}