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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesPageTokenKt as InternalPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequest as InternalListCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageTokenKt as InternalFilesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageTokenKt as InternalUploadsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequest as InternalListUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidateGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt as InternalFileGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt as InternalUploadGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState as InternalUploadState
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile

@RunWith(JUnit4::class)
class RawImpressionUploadCorrectionCandidateServiceTest {
  private val internalCandidateService:
    InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase =
    mockService()
  private val internalUploadService:
    InternalUploadGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val internalFileService:
    InternalFileGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalCandidateService)
    addService(internalUploadService)
    addService(internalFileService)
  }

  private val service by lazy {
    RawImpressionUploadCorrectionCandidateService(
      InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(
        grpcTestServerRule.channel
      ),
      InternalUploadGrpcKt.RawImpressionUploadServiceCoroutineStub(grpcTestServerRule.channel),
      InternalFileGrpcKt.RawImpressionUploadFileServiceCoroutineStub(grpcTestServerRule.channel),
    )
  }

  @Test
  fun `get returns manifest differences with historical owners`() =
    runBlocking<Unit> {
      whenever(internalCandidateService.getRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(INTERNAL_CANDIDATE)
      val currentUpload = internalUpload(CURRENT_UPLOAD_ID, 4L, PREVIOUS_UPLOAD_ID)
      val previousUpload = internalUpload(PREVIOUS_UPLOAD_ID, 3L, BASE_UPLOAD_ID)
      val baseUpload = internalUpload(BASE_UPLOAD_ID, 2L, LEGACY_UPLOAD_ID, "healing-operation")
      val legacyUpload = internalUpload(LEGACY_UPLOAD_ID, 1L)
      whenever(internalUploadService.getRawImpressionUpload(any())).thenReturn(currentUpload)
      val nextUploadsPageToken = listRawImpressionUploadsPageToken {
        after =
          InternalUploadsPageTokenKt.after {
            createTime = timestamp { seconds = 3L }
            rawImpressionUploadResourceId = PREVIOUS_UPLOAD_ID
          }
      }
      whenever(internalUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += listOf(currentUpload, previousUpload)
            nextPageToken = nextUploadsPageToken
          },
          listRawImpressionUploadsResponse {
            rawImpressionUploads += listOf(baseUpload, legacyUpload)
          },
        )
      val nextFilesPageToken = listRawImpressionUploadFilesPageToken {
        after =
          InternalFilesPageTokenKt.after {
            createTime = timestamp { seconds = 4L }
            rawImpressionUploadResourceId = CURRENT_UPLOAD_ID
            fileResourceId = "file-b"
          }
      }
      whenever(internalFileService.listRawImpressionUploadFiles(any())).thenAnswer { invocation ->
        val request =
          invocation.getArgument<
            org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequest
          >(
            0
          )
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles +=
            when (request.rawImpressionUploadResourceId) {
              CURRENT_UPLOAD_ID -> {
                if (request.hasPageToken()) {
                  listOf(internalFile(CURRENT_UPLOAD_ID, "gs://raw/d", 1L, 4))
                } else {
                  nextPageToken = nextFilesPageToken
                  listOf(
                    internalFile(CURRENT_UPLOAD_ID, "gs://raw/a", 1L, 1),
                    internalFile(CURRENT_UPLOAD_ID, "gs://raw/b", 2L, 2),
                  )
                }
              }
              PREVIOUS_UPLOAD_ID -> listOf(internalFile(PREVIOUS_UPLOAD_ID, "gs://raw/b", 1L, 1))
              BASE_UPLOAD_ID ->
                listOf(
                  internalFile(BASE_UPLOAD_ID, "gs://raw/a", 1L, 1),
                  internalFile(BASE_UPLOAD_ID, "gs://raw/c", 1L, 3),
                )
              LEGACY_UPLOAD_ID -> listOf(internalFile(LEGACY_UPLOAD_ID, "gs://raw/z", 1L, 1))
              else -> error("unexpected upload")
            }
        }
      }

      val result =
        service.getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest { name = CANDIDATE_NAME }
        )

      assertThat(result.name).isEqualTo(CANDIDATE_NAME)
      assertThat(result.manifestDifferencesList.map { it.blobUri })
        .containsExactly("gs://raw/b", "gs://raw/c", "gs://raw/d")
        .inOrder()
      val edited = result.manifestDifferencesList[0]
      assertThat(edited.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED)
      assertThat(edited.priorBlobGeneration).isEqualTo(1L)
      assertThat(edited.currentBlobGeneration).isEqualTo(2L)
      assertThat(edited.historicalOwnerRawImpressionUpload).isEqualTo(PREVIOUS_UPLOAD)
      val removed = result.manifestDifferencesList[1]
      assertThat(removed.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED)
      assertThat(removed.historicalOwnerRawImpressionUpload).isEqualTo(BASE_UPLOAD)
      val added = result.manifestDifferencesList[2]
      assertThat(added.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED)
      assertThat(added.historicalOwnerRawImpressionUpload).isEmpty()
      val uploadRequests = argumentCaptor<InternalListUploadsRequest>()
      verify(internalUploadService, org.mockito.kotlin.times(2))
        .listRawImpressionUploads(uploadRequests.capture())
      assertThat(uploadRequests.secondValue.pageToken).isEqualTo(nextUploadsPageToken)
    }

  @Test
  fun `list forwards pagination and filters without loading raw manifests`() =
    runBlocking<Unit> {
      val nextPageToken = listRawImpressionUploadCorrectionCandidatesPageToken {
        after =
          InternalPageTokenKt.after {
            createTime = timestamp { seconds = 10L }
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          }
      }
      whenever(internalCandidateService.listRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(
          listRawImpressionUploadCorrectionCandidatesResponse {
            rawImpressionUploadCorrectionCandidates += INTERNAL_CANDIDATE
            this.nextPageToken = nextPageToken
          }
        )

      val response =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            parent = DATA_PROVIDER
            pageSize = 25
            filter =
              org.wfanet.measurement.edpaggregator.v1alpha
                .ListRawImpressionUploadCorrectionCandidatesRequestKt
                .filter {
                  stateIn += RawImpressionUploadCorrectionCandidate.State.PLANNED
                  classificationIn += RawImpressionUploadCorrectionCandidate.Classification.MIXED
                }
          }
        )

      val request = argumentCaptor<InternalListCandidatesRequest>()
      verify(internalCandidateService)
        .listRawImpressionUploadCorrectionCandidates(request.capture())
      assertThat(request.firstValue.pageSize).isEqualTo(25)
      assertThat(request.firstValue.filter.stateInList)
        .containsExactly(InternalCandidate.State.STATE_PLANNED)
      assertThat(request.firstValue.filter.classificationInList)
        .containsExactly(InternalCandidate.Classification.CLASSIFICATION_MIXED)
      assertThat(
          response.rawImpressionUploadCorrectionCandidatesList.single().manifestDifferencesList
        )
        .isEmpty()
      assertThat(response.toString()).doesNotContain("gs://")
      assertThat(response.nextPageToken).isEqualTo(nextPageToken.toByteArray().base64UrlEncode())
      verify(internalUploadService, never()).getRawImpressionUpload(any())
      verify(internalFileService, never()).listRawImpressionUploadFiles(any())
    }

  @Test
  fun `list forwards decoded page token`() =
    runBlocking<Unit> {
      val pageToken = listRawImpressionUploadCorrectionCandidatesPageToken {
        after =
          InternalPageTokenKt.after {
            createTime = timestamp { seconds = 10L }
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          }
      }
      whenever(internalCandidateService.listRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(listRawImpressionUploadCorrectionCandidatesResponse {})

      service.listRawImpressionUploadCorrectionCandidates(
        listRawImpressionUploadCorrectionCandidatesRequest {
          parent = DATA_PROVIDER
          this.pageToken = pageToken.toByteArray().base64UrlEncode()
        }
      )

      val request = argumentCaptor<InternalListCandidatesRequest>()
      verify(internalCandidateService)
        .listRawImpressionUploadCorrectionCandidates(request.capture())
      assertThat(request.firstValue.pageToken).isEqualTo(pageToken)
    }

  @Test
  fun `get rejects malformed name`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusRuntimeException> {
          service.getRawImpressionUploadCorrectionCandidate(
            getRawImpressionUploadCorrectionCandidateRequest { name = "not-a-resource-name" }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `list rejects malformed page token`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadCorrectionCandidates(
            listRawImpressionUploadCorrectionCandidatesRequest {
              parent = DATA_PROVIDER
              pageToken = "not-base64"
            }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  private fun internalFile(uploadId: String, uri: String, generation: Long, day: Int) =
    rawImpressionUploadFile {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUploadResourceId = uploadId
      blobUri = uri
      blobGeneration = generation
      eventDate = date {
        year = 2026
        month = 9
        this.day = day
      }
    }

  private fun internalUpload(
    uploadId: String,
    order: Long,
    replaces: String = "",
    evictionOperationId: String = "",
  ) = rawImpressionUpload {
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadResourceId = uploadId
    doneBlobUri = DONE_BLOB_URI
    doneBlobGeneration = order
    doneBlobCreateTime = timestamp { seconds = order }
    createTime = timestamp { seconds = order }
    replacesRawImpressionUploadResourceId = replaces
    this.evictionOperationId = evictionOperationId
    registrationComplete = true
    state = InternalUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
  }

  companion object {
    private const val DATA_PROVIDER_ID = "dp"
    private const val DATA_PROVIDER = "dataProviders/$DATA_PROVIDER_ID"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val CANDIDATE_NAME =
      "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
    private const val CURRENT_UPLOAD_ID = "current"
    private const val PREVIOUS_UPLOAD_ID = "previous"
    private const val BASE_UPLOAD_ID = "base"
    private const val LEGACY_UPLOAD_ID = "legacy"
    private const val DONE_BLOB_URI = "gs://raw/done"
    private const val PREVIOUS_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/$PREVIOUS_UPLOAD_ID"
    private const val BASE_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/$BASE_UPLOAD_ID"
    private val INTERNAL_CANDIDATE = internalCandidate {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUploadResourceId = CURRENT_UPLOAD_ID
      classification = InternalCandidate.Classification.CLASSIFICATION_MIXED
      priorManifestDigest = ByteString.copyFrom(ByteArray(32) { 1 })
      currentManifestDigest = ByteString.copyFrom(ByteArray(32) { 2 })
      state = InternalCandidate.State.STATE_PLANNED
      createTime = timestamp { seconds = 1L }
      updateTime = timestamp { seconds = 2L }
      expireTime = timestamp { seconds = 3L }
      etag = "etag"
    }
  }
}
