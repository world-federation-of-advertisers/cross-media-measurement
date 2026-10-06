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
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt as InternalCandidateKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidateGrpcKt
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate

@RunWith(JUnit4::class)
class RawImpressionUploadCorrectionCandidateServiceTest {
  private val internalCandidateService:
    InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase =
    mockService()
  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalCandidateService) }

  private val service by lazy {
    RawImpressionUploadCorrectionCandidateService(
      InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(
        grpcTestServerRule.channel
      )
    )
  }

  @Test
  fun `get returns persisted manifest differences after lifecycle changes`() =
    runBlocking<Unit> {
      whenever(internalCandidateService.getRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          INTERNAL_CANDIDATE,
          INTERNAL_CANDIDATE.copy {
            state = InternalCandidate.State.STATE_HEALING
            uploadHealingOperationId = "operation"
          },
        )
      val beforeLifecycleChange =
        service.getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest { name = CANDIDATE_NAME }
        )
      val afterLifecycleChange =
        service.getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest { name = CANDIDATE_NAME }
        )

      assertThat(beforeLifecycleChange.name).isEqualTo(CANDIDATE_NAME)
      assertThat(afterLifecycleChange.manifestDifferencesList)
        .containsExactlyElementsIn(beforeLifecycleChange.manifestDifferencesList)
        .inOrder()
      assertThat(afterLifecycleChange.manifestDifferencesList.map { it.blobUri })
        .containsExactly("gs://raw/b", "gs://raw/c", "gs://raw/d")
        .inOrder()
      val edited = afterLifecycleChange.manifestDifferencesList[0]
      assertThat(edited.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED)
      assertThat(edited.priorBlobGeneration).isEqualTo(1L)
      assertThat(edited.currentBlobGeneration).isEqualTo(2L)
      assertThat(edited.historicalOwnerRawImpressionUpload).isEqualTo(PREVIOUS_UPLOAD)
      val removed = afterLifecycleChange.manifestDifferencesList[1]
      assertThat(removed.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED)
      assertThat(removed.historicalOwnerRawImpressionUpload).isEqualTo(BASE_UPLOAD)
      val added = afterLifecycleChange.manifestDifferencesList[2]
      assertThat(added.type)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED)
      assertThat(added.historicalOwnerRawImpressionUpload).isEmpty()
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

  companion object {
    private const val DATA_PROVIDER_ID = "dp"
    private const val DATA_PROVIDER = "dataProviders/$DATA_PROVIDER_ID"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val CANDIDATE_NAME =
      "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID"
    private const val CURRENT_UPLOAD_ID = "current"
    private const val PREVIOUS_UPLOAD_ID = "previous"
    private const val BASE_UPLOAD_ID = "base"
    private const val PREVIOUS_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/$PREVIOUS_UPLOAD_ID"
    private const val BASE_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/$BASE_UPLOAD_ID"
    private val INTERNAL_CANDIDATE = internalCandidate {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
      rawImpressionUploadResourceId = CURRENT_UPLOAD_ID
      classification = InternalCandidate.Classification.CLASSIFICATION_MIXED
      priorManifestDigest = ByteString.copyFrom(ByteArray(32) { 1 })
      currentManifestDigest = ByteString.copyFrom(ByteArray(32) { 2 })
      manifestComparison =
        InternalCandidateKt.manifestComparison {
          priorManifest +=
            listOf(
              manifestEntry("gs://raw/a", 1L, 1, BASE_UPLOAD_ID),
              manifestEntry("gs://raw/b", 1L, 1, PREVIOUS_UPLOAD_ID),
              manifestEntry("gs://raw/c", 1L, 3, BASE_UPLOAD_ID),
            )
          currentManifest +=
            listOf(
              manifestEntry("gs://raw/a", 1L, 1, CURRENT_UPLOAD_ID),
              manifestEntry("gs://raw/b", 2L, 2, CURRENT_UPLOAD_ID),
              manifestEntry("gs://raw/d", 1L, 4, CURRENT_UPLOAD_ID),
            )
          differences +=
            listOf(
              InternalCandidateKt.manifestDifference {
                type = InternalCandidate.ManifestDifference.Type.TYPE_EDITED
                prior = manifestEntry("gs://raw/b", 1L, 1, PREVIOUS_UPLOAD_ID)
                current = manifestEntry("gs://raw/b", 2L, 2, CURRENT_UPLOAD_ID)
              },
              InternalCandidateKt.manifestDifference {
                type = InternalCandidate.ManifestDifference.Type.TYPE_REMOVED
                prior = manifestEntry("gs://raw/c", 1L, 3, BASE_UPLOAD_ID)
              },
              InternalCandidateKt.manifestDifference {
                type = InternalCandidate.ManifestDifference.Type.TYPE_ADDED
                current = manifestEntry("gs://raw/d", 1L, 4, CURRENT_UPLOAD_ID)
              },
            )
        }
      state = InternalCandidate.State.STATE_PLANNED
      createTime = timestamp { seconds = 1L }
      updateTime = timestamp { seconds = 2L }
      expireTime = timestamp { seconds = 3L }
      etag = "etag"
    }

    private fun manifestEntry(uri: String, generation: Long, day: Int, owner: String) =
      InternalCandidateKt.manifestEntry {
        blobUri = uri
        blobGeneration = generation
        eventDate = date {
          year = 2026
          month = 1
          this.day = day
        }
        ownerRawImpressionUploadResourceId = owner
      }
  }
}
