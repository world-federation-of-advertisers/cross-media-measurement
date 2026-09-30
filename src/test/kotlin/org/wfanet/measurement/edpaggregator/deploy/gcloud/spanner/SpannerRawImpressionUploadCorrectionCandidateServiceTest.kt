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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.AdvanceRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.advanceRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate

@RunWith(JUnit4::class)
class SpannerRawImpressionUploadCorrectionCandidateServiceTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private val service by lazy {
    SpannerRawImpressionUploadCorrectionCandidateService(spannerDatabase.databaseClient)
  }

  @Test
  fun `create persists candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )

      val created = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val fetched =
        service.getRawImpressionUploadCorrectionCandidate(
          getRawImpressionUploadCorrectionCandidateRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            rawImpressionUploadCorrectionCandidateId = CANDIDATE_ID
          }
        )

      assertThat(created).isEqualTo(fetched)
      assertThat(created.dataProviderResourceId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(created.rawImpressionUploadCorrectionCandidateId).isEqualTo(CANDIDATE_ID)
      assertThat(created.rawImpressionUploadResourceId).isEqualTo(UPLOAD_ID)
      assertThat(created.classification)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED)
      assertThat(created.priorManifestDigest).isEqualTo(PRIOR_DIGEST)
      assertThat(created.currentManifestDigest).isEqualTo(CURRENT_DIGEST)
      assertThat(created.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
      assertThat(created.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
      assertThat(created.hasCreateTime()).isTrue()
      assertThat(created.hasUpdateTime()).isTrue()
      assertThat(created.etag).isNotEmpty()
    }

  @Test
  fun `create is idempotent`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val request = createRequest()

      val first = service.createRawImpressionUploadCorrectionCandidate(request)
      val second = service.createRawImpressionUploadCorrectionCandidate(request)

      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `create rejects reused request ID with different candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertRawUpload(
        2L,
        UPLOAD_ID_2,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(
            createRequest(CANDIDATE_ID_2, UPLOAD_ID_2).copy { requestId = CREATE_REQUEST_ID }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `create rejects upload that is not quarantined`() =
    runBlocking<Unit> {
      insertRawUpload(1L, UPLOAD_ID, RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects quarantined upload before manifest registration completes`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        registrationComplete = false,
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects quarantined upload without done creation time`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        includeDoneBlobCreateTime = false,
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(createRequest())
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `create rejects invalid manifest digest`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val request =
        createRequest().copy {
          rawImpressionUploadCorrectionCandidate =
            rawImpressionUploadCorrectionCandidate.copy {
              priorManifestDigest = ByteString.copyFromUtf8("not-sha256")
            }
        }

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(request)
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create rejects second candidate for same upload`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadCorrectionCandidate(
            createRequest(CANDIDATE_ID_2, UPLOAD_ID)
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `list paginates in creation order`() =
    runBlocking<Unit> {
      createCandidates(3)

      val first =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 2
          }
        )
      val second =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 2
            pageToken = first.nextPageToken
          }
        )

      assertThat(
          first.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[0], CANDIDATE_IDS[1])
        .inOrder()
      assertThat(first.hasNextPageToken()).isTrue()
      assertThat(
          second.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[2])
      assertThat(second.hasNextPageToken()).isFalse()
    }

  @Test
  fun `list filters by state`() =
    runBlocking<Unit> {
      createCandidates(2)
      val pending = getCandidate(CANDIDATE_IDS[0])
      service.advanceRawImpressionUploadCorrectionCandidate(
        advanceRequest(
          CANDIDATE_IDS[0],
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      )

      val response =
        service.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            filter =
              ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
                stateIn +=
                  RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
              }
          }
        )

      assertThat(
          response.rawImpressionUploadCorrectionCandidatesList.map {
            it.rawImpressionUploadCorrectionCandidateId
          }
        )
        .containsExactly(CANDIDATE_IDS[0])
    }

  @Test
  fun `list rejects negative page size`() =
    runBlocking<Unit> {
      val error =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadCorrectionCandidates(
            listRawImpressionUploadCorrectionCandidatesRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              pageSize = -1
            }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `correct lifecycle completes`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val planned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            planned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT,
          )
        )
      val healing =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            approved.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
          )
        )
      completeHealingOperation()
      val complete =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            healing.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
          )
        )

      assertThat(planned.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(approved.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT)
      assertThat(complete.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE)
    }

  @Test
  fun `no replacement lifecycle completes without replay state`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val planned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            planned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT,
          )
        )
      val healing =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            approved.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
          )
        )

      completeHealingOperation()
      val complete =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            healing.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
          )
        )

      assertThat(complete.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_NO_REPLACEMENT)
      assertThat(complete.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT)
    }

  @Test
  fun `advance is idempotent`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val request =
        advanceRequest(
          CANDIDATE_ID,
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )

      val first = service.advanceRawImpressionUploadCorrectionCandidate(request)
      val second = service.advanceRawImpressionUploadCorrectionCandidate(request)
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            first.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT,
          )
        )
      val delayedRetry = service.advanceRawImpressionUploadCorrectionCandidate(request)

      assertThat(second).isEqualTo(first)
      assertThat(delayedRetry).isEqualTo(approved)
    }

  @Test
  fun `advance rejects request ID reused for another action`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val assignRequest =
        advanceRequest(
          CANDIDATE_ID,
          pending.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )
      val planned = service.advanceRawImpressionUploadCorrectionCandidate(assignRequest)

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            assignRequest.copy {
              etag = planned.etag
              action = AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT
              uploadHealingOperationId = ""
            }
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `advance rejects request ID reused for another candidate`() =
    runBlocking<Unit> {
      createCandidates(2)
      insertHealingOperation()
      val first = getCandidate(CANDIDATE_IDS[0])
      val second = getCandidate(CANDIDATE_IDS[1])
      service.advanceRawImpressionUploadCorrectionCandidate(
        advanceRequest(
          CANDIDATE_IDS[0],
          first.etag,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          operationId = OPERATION_ID,
        )
      )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_IDS[1],
              second.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `assign plan rejects missing healing operation`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              pending.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `complete rejects active healing operation`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      insertHealingOperation()
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val planned =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
            operationId = OPERATION_ID,
          )
        )
      val approved =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            planned.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT,
          )
        )
      val healing =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            approved.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
          )
        )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              healing.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `advance rejects stale etag`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              "stale",
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
              operationId = OPERATION_ID,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.ABORTED)
    }

  @Test
  fun `advance rejects invalid lifecycle transition`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              pending.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `supersede records newer candidate`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_IDS[0],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        doneBlobUri = SHARED_DONE_BLOB_URI,
      )
      insertRawUpload(
        2L,
        UPLOAD_IDS[1],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
        doneBlobUri = SHARED_DONE_BLOB_URI,
      )
      val current =
        service.createRawImpressionUploadCorrectionCandidate(
          createRequest(CANDIDATE_IDS[0], UPLOAD_IDS[0], CREATE_REQUEST_IDS[0])
        )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(CANDIDATE_IDS[1], UPLOAD_IDS[1], CREATE_REQUEST_IDS[1])
      )

      val superseded =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_IDS[0],
            current.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
            supersedingCandidateId = CANDIDATE_IDS[1],
          )
        )

      assertThat(superseded.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
      assertThat(superseded.supersedingRawImpressionUploadCorrectionCandidateId)
        .isEqualTo(CANDIDATE_IDS[1])
    }

  @Test
  fun `supersede rejects candidate for another done object`() =
    runBlocking<Unit> {
      createCandidates(2)
      val current = getCandidate(CANDIDATE_IDS[0])

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_IDS[0],
              current.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
              supersedingCandidateId = CANDIDATE_IDS[1],
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `terminal candidate rejects further transition`() =
    runBlocking<Unit> {
      insertRawUpload(
        1L,
        UPLOAD_ID,
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      val pending = service.createRawImpressionUploadCorrectionCandidate(createRequest())
      val rejected =
        service.advanceRawImpressionUploadCorrectionCandidate(
          advanceRequest(
            CANDIDATE_ID,
            pending.etag,
            AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT,
          )
        )

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.advanceRawImpressionUploadCorrectionCandidate(
            advanceRequest(
              CANDIDATE_ID,
              rejected.etag,
              AdvanceRawImpressionUploadCorrectionCandidateRequest.Action
                .REQUIRE_MANUAL_INTERVENTION,
            )
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  private suspend fun createCandidates(count: Int) {
    for (index in 0 until count) {
      insertRawUpload(
        index.toLong() + 1L,
        UPLOAD_IDS[index],
        RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED,
      )
      service.createRawImpressionUploadCorrectionCandidate(
        createRequest(CANDIDATE_IDS[index], UPLOAD_IDS[index], CREATE_REQUEST_IDS[index])
      )
    }
  }

  private suspend fun getCandidate(candidateId: String): RawImpressionUploadCorrectionCandidate =
    service.getRawImpressionUploadCorrectionCandidate(
      getRawImpressionUploadCorrectionCandidateRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        rawImpressionUploadCorrectionCandidateId = candidateId
      }
    )

  private fun createRequest(
    candidateId: String = CANDIDATE_ID,
    uploadId: String = UPLOAD_ID,
    requestId: String = CREATE_REQUEST_ID,
  ) = createRawImpressionUploadCorrectionCandidateRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadCorrectionCandidateId = candidateId
    rawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
      rawImpressionUploadResourceId = uploadId
      classification = RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_MIXED
      priorManifestDigest = PRIOR_DIGEST
      currentManifestDigest = CURRENT_DIGEST
      expireTime = EXPIRY_TIME
    }
    this.requestId = requestId
  }

  private fun advanceRequest(
    candidateId: String,
    etag: String,
    action: AdvanceRawImpressionUploadCorrectionCandidateRequest.Action,
    operationId: String = "",
    supersedingCandidateId: String = "",
  ) = advanceRawImpressionUploadCorrectionCandidateRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    rawImpressionUploadCorrectionCandidateId = candidateId
    this.etag = etag
    this.action = action
    uploadHealingOperationId = operationId
    supersedingRawImpressionUploadCorrectionCandidateId = supersedingCandidateId
    requestId = REQUEST_IDS.getValue(action)
  }

  private suspend fun insertRawUpload(
    internalId: Long,
    resourceId: String,
    state: RawImpressionUploadState,
    doneBlobUri: String = "gs://bucket/$resourceId/done",
    registrationComplete: Boolean = true,
    includeDoneBlobCreateTime: Boolean = true,
  ) {
    val mutation =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(internalId)
        .set("RawImpressionUploadResourceId")
        .to(resourceId)
        .set("DoneBlobUri")
        .to(doneBlobUri)
        .set("DoneBlobGeneration")
        .to(internalId)
        .set("RegistrationComplete")
        .to(registrationComplete)
        .set("State")
        .to(Value.protoEnum(state))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
    if (includeDoneBlobCreateTime) {
      mutation.set("DoneBlobCreateTime").to(Timestamp.ofTimeSecondsAndNanos(internalId, 0))
    }
    spannerDatabase.databaseClient.write(listOf(mutation.build()))
  }

  private suspend fun insertHealingOperation() {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("UploadHealingOperation")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("CreateRequestId")
          .to(HEALING_CREATE_REQUEST_ID)
          .set("Reason")
          .to("correction")
          .set("LabeledImpressionsBlobPrefix")
          .to("gs://bucket/labeled")
          .set("BadRawImpressionUploadResourceIds")
          .toStringArray(listOf(UPLOAD_ID))
          .set("CutoffTime")
          .to(Timestamp.ofTimeSecondsAndNanos(1L, 0))
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private suspend fun completeHealingOperation() {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("UploadHealingOperation")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("CompleteTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val UPLOAD_ID = "upload-1"
    private const val UPLOAD_ID_2 = "upload-2"
    private const val CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
    private const val CANDIDATE_ID_2 = "22222222-2222-4222-8222-222222222222"
    private const val CREATE_REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val OPERATION_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val HEALING_CREATE_REQUEST_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    private const val SHARED_DONE_BLOB_URI = "gs://bucket/shared/done"
    private val UPLOAD_IDS = listOf(UPLOAD_ID, UPLOAD_ID_2, "upload-3")
    private val CANDIDATE_IDS =
      listOf(CANDIDATE_ID, CANDIDATE_ID_2, "33333333-3333-4333-8333-333333333333")
    private val CREATE_REQUEST_IDS =
      listOf(
        CREATE_REQUEST_ID,
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      )
    private val REQUEST_IDS =
      mapOf(
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN to
          "00000000-0000-4000-8000-000000000001",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT to
          "00000000-0000-4000-8000-000000000002",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT to
          "00000000-0000-4000-8000-000000000003",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING to
          "00000000-0000-4000-8000-000000000004",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE to
          "00000000-0000-4000-8000-000000000005",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT to
          "00000000-0000-4000-8000-000000000006",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE to
          "00000000-0000-4000-8000-000000000007",
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION to
          "00000000-0000-4000-8000-000000000008",
      )
    private val PRIOR_DIGEST = ByteString.copyFrom(ByteArray(32) { 1 })
    private val CURRENT_DIGEST = ByteString.copyFrom(ByteArray(32) { 2 })
    private val EXPIRY_TIME = timestamp { seconds = 4_102_444_800L }
  }
}
