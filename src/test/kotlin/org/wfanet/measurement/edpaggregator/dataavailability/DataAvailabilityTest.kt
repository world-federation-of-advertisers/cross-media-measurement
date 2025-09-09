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
import org.mockito.kotlin.mock
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import kotlinx.coroutines.runBlocking
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import java.time.Clock
import java.time.Duration
import org.mockito.kotlin.times

@RunWith(JUnit4::class)
class DataAvailabilityTest {

    private val bucket = "my-bucket"
    private val folderPrefix = "some/prefix"

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

    @Test
    fun `register single contiguous day for existing model line`() {

       val storageClient = mockGoogleStorage(listOf(
           300L to 400L,
       ))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `register single overlapping day for existing model line`() {

        val storageClient = mockGoogleStorage(listOf(
            250L to 400L,
        ))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `registers a single contiguous day preceding an existing interval for an existing model line`() {

        val storageClient = mockGoogleStorage(listOf(
            50L to 100L,
        ))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `register multiple contiguous day for existing model line`() {

        val storageClient = mockGoogleStorage(listOf(
            300L to 400L,
            400L to 500L,
            500L to 600L
        ))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(3)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `blob details with missing interval is ignored`() {

        val storageClient = mockGoogleStorage(listOf(null to null))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    @Test
    fun `invalid single interval fails for existing model line`() {

        val storageClient = mockGoogleStorage(listOf(
            320L to 400L
        ))

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersStub,
            impressionMetadataStub,
            "dataProviders/dataProvider123",
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
        )

        runBlocking { dataAvailability.sync("$bucket/$folderPrefix/file.metadata") }
        verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(0)) { createImpressionMetadata(any()) }
        verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    }

    fun mockGoogleStorage(intervals: List<Pair<Long?, Long?>>): Storage {
        val mockBlobs = intervals.mapIndexed { index, (startSeconds, endSeconds) ->
            val blobDetails = blobDetails {
                blobUri = "some_blob_uri_$index"
                eventGroupReferenceId = "event${index + 1}"
                modelLine = "modelLine1"
                interval = interval {
                    if (startSeconds != null) {
                        startTime = timestamp {
                            seconds = startSeconds
                            nanos = 0
                        }
                    }
                    if (endSeconds != null) {
                        endTime = timestamp {
                            seconds = endSeconds
                            nanos = 0
                        }
                    }
                }
            }

            mock<Blob> {
                on { name } doReturn "$folderPrefix/file${index + 1}"
                on { getContent() } doReturn blobDetails.toByteArray()
            }
        }

        val mockPage = mock<Page<Blob>> {
            on { iterateAll() } doReturn mockBlobs
        }

        return mock {
            on { list(eq(bucket), any<Storage.BlobListOption>()) } doReturn mockPage
        }
    }

}