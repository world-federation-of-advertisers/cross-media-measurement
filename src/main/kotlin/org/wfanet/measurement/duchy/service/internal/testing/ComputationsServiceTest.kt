// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.util.Durations
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.reset
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest.AfterTransition
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.honestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.advanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.deleteComputationRequest
import org.wfanet.measurement.internal.duchy.enqueueComputationRequest
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.finishComputationRequest
import org.wfanet.measurement.internal.duchy.getComputationIdsRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant
import org.wfanet.measurement.internal.duchy.protocol.copy
import org.wfanet.measurement.internal.duchy.purgeComputationsRequest
import org.wfanet.measurement.internal.duchy.recordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.recordRequisitionFulfillmentRequest
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionEntry
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.computationLogEntry
import org.wfanet.measurement.system.v1alpha.createComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.stageAttempt

@RunWith(JUnit4::class)
abstract class ComputationsServiceTest<T : ComputationsCoroutineImplBase> {
  protected val mockComputationLogEntriesService: ComputationLogEntriesCoroutineImplBase =
    mockService()

  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(clock: Clock): T

  private val clock =
    TestClockWithNamedInstants(
      Clock.systemUTC().instant().minusSeconds(10).truncatedTo(ChronoUnit.MICROS)
    )

  @Before
  fun initService() {
    service = newService(clock)
  }

  companion object {
    const val DUCHY_ID = "worker1"
    private const val GLOBAL_COMPUTATION_ID = "1234"
    private const val REQUISITION_ID = "1234"
    private const val REQUISITION_FINGERPRINT = "finger_print"
    private const val REQUISITION_NONCE = 1234L
    private val AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
      liquidLegionsV2 =
        LiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.AGGREGATOR
        }
    }
    private val DEFAULT_REQUISITION_ENTRY = requisitionEntry {
      key = externalRequisitionKey {
        externalRequisitionId = REQUISITION_ID
        requisitionFingerprint = REQUISITION_FINGERPRINT.toByteStringUtf8()
      }
      value = requisitionDetails {
        nonce = REQUISITION_NONCE
        externalFulfillingDuchyId = REQUISITION_ID
        nonceHash = REQUISITION_NONCE.toByteString()
      }
    }
    private val DEFAULT_CREATE_COMPUTATION_REQUEST = createComputationRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      globalComputationId = GLOBAL_COMPUTATION_ID
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS
      requisitions += DEFAULT_REQUISITION_ENTRY
    }
    private val DEFAULT_CREATE_COMPUTATION_RESP_TOKEN = computationToken {
      globalComputationId = GLOBAL_COMPUTATION_ID
      computationStage = computationStage {
        liquidLegionsSketchAggregationV2 = Stage.INITIALIZATION_PHASE
      }
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS
      stageSpecificDetails = ComputationStageDetails.getDefaultInstance()
      requisitions += requisitionMetadata {
        externalKey = DEFAULT_REQUISITION_ENTRY.key
        details = DEFAULT_REQUISITION_ENTRY.value
      }
    }
    private const val DEFAULT_OWNER = "$DUCHY_ID-duchy-mill-123"
    private val DEFAULT_CLAIM_WORK_REQUEST = claimWorkRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      owner = DEFAULT_OWNER
    }
  }

  @Test
  fun `createComputation returns response with token`() = runBlocking {
    val writeTime: Instant = clock.last()

    val response: CreateComputationResponse =
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    assertThat(response.token.localComputationId).isNotEqualTo(0L)
    assertThat(response.token.version).isNotEqualTo(0L)
    assertThat(response.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER,
      )
      .isEqualTo(
        DEFAULT_CREATE_COMPUTATION_RESP_TOKEN.copy { lockExpirationTime = writeTime.toProtoTime() }
      )
  }

  @Test
  fun `createComputation throws ALREADY_EXISTS when called with existing ID`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `deleteComputation returns empty proto when succeeded`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val deleteRequest = deleteComputationRequest {
      localComputationId = createComputationResp.token.localComputationId
    }
    assertThat(service.deleteComputation(deleteRequest)).isEqualToDefaultInstance()
  }

  @Test
  fun `deleteComputation returns empty proto when called with nonexistent ID`() = runBlocking {
    val doesNotExistComputationId = 1234L

    // TODO(world-federation-of-advertisers/cross-media-measurement#889): deleteComputation should
    // throw NOT_FOUND exception rather than return empty response.
    val deleteRequest = deleteComputationRequest { localComputationId = doesNotExistComputationId }
    assertThat(service.deleteComputation(deleteRequest)).isEqualToDefaultInstance()
  }

  @Test
  fun `purgeComputations returns the matched computation IDs when force is false`() = runBlocking {
    val token1 = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    val token2 =
      service
        .createComputation(
          DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
            globalComputationId = "5678"
            requisitions[0] =
              requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
          }
        )
        .token
    service.finishComputation(
      finishComputationRequest {
        token = token1
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )
    service.finishComputation(
      finishComputationRequest {
        token = token2
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )

    val currentTime = clock.last()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.COMPLETE.toProtocolStage()
          force = false
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList)
      .containsExactly(token1.globalComputationId, token2.globalComputationId)
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `purgeComputations does not delete Computations when force is false`() = runBlocking {
    val createTime = clock.last()
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    val finishedToken =
      service
        .finishComputation(
          finishComputationRequest {
            token = createdToken
            endingComputationStage = Stage.COMPLETE.toProtocolStage()
            reason = ComputationDetails.CompletedReason.SUCCEEDED
          }
        )
        .token

    service.purgeComputations(
      purgeComputationsRequest {
        updatedBefore = createTime.plusSeconds(1000L).toProtoTime()
        stages += Stage.COMPLETE.toProtocolStage()
        force = false
      }
    )

    assertThat(
        service
          .getComputationToken(
            getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
          )
          .token
      )
      .isEqualTo(finishedToken)
  }

  @Test
  fun `purgeComputations only returns the deleted count when force is true`() = runBlocking {
    val token1 = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    val token2 =
      service
        .createComputation(
          DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
            globalComputationId = "5678"
            requisitions[0] =
              requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
          }
        )
        .token
    service.finishComputation(
      finishComputationRequest {
        token = token1
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )
    service.finishComputation(
      finishComputationRequest {
        token = token2
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )

    val currentTime = clock.last()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.COMPLETE.toProtocolStage()
          force = true
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList).isEmpty()
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `purgeComputations throws INVALID_ARGUMENT exception when target stage is non-terminal`() =
    runBlocking {
      val currentTime = clock.last()
      val purgeComputationsRequest = purgeComputationsRequest {
        updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
        stages += Stage.COMPLETE.toProtocolStage()
        stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
        force = true
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.purgeComputations(purgeComputationsRequest)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `purgeComputations only deletes computations of target stages`() = runBlocking {
    // Creates a computation in WAIT_REQUISITIONS_AND_KEY_SET stage
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)
    val nextStage = computationStage {
      liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
    }
    val advanceComputationStageRequest = advanceComputationStageRequest {
      token = claimWorkResponse.token
      nextComputationStage = nextStage
      afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
    }
    service.advanceComputationStage(advanceComputationStageRequest)
    // Creates two computations in COMPLETE stage to be purged
    val token1 =
      service
        .createComputation(
          DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
            globalComputationId = "3456"
            requisitions[0] =
              requisitions[0].copy { key = key.copy { externalRequisitionId = "3456" } }
          }
        )
        .token
    val token2 =
      service
        .createComputation(
          DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
            globalComputationId = "5678"
            requisitions[0] =
              requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
          }
        )
        .token
    service.finishComputation(
      finishComputationRequest {
        token = token1
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )
    service.finishComputation(
      finishComputationRequest {
        token = token2
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )

    val currentTime = clock.last()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.COMPLETE.toProtocolStage()
          force = false
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList)
      .containsExactly(token1.globalComputationId, token2.globalComputationId)
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `getComputationToken throws NOT_FOUND when computation is purged`() = runBlocking {
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    service.finishComputation(
      finishComputationRequest {
        token = createdToken
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
    )

    val currentTime = clock.last()
    val purgeComputationsRequest = purgeComputationsRequest {
      updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
      stages += Stage.COMPLETE.toProtocolStage()
      force = true
    }
    service.purgeComputations(purgeComputationsRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getComputationToken(
          getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getComputationToken by global computation ID returns created computation`() = runBlocking {
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token

    val getComputationResponse =
      service.getComputationToken(
        getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
      )

    assertThat(getComputationResponse.token).isEqualTo(createdToken)
  }

  @Test
  fun `getComputationToken by requisition key returns created computation`() = runBlocking {
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token

    val response =
      service.getComputationToken(
        getComputationTokenRequest { requisitionKey = DEFAULT_REQUISITION_ENTRY.key }
      )

    assertThat(response.token).isEqualTo(createdToken)
  }

  @Test
  fun `getComputationToken throws INVALID_ARGUMENT when request key is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getComputationToken(GetComputationTokenRequest.getDefaultInstance())
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("key not set")
  }

  @Test
  fun `getComputationToken throws NOT_FOUND for deleted computation`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val deleteComputationRequest = deleteComputationRequest {
      localComputationId = createComputationResp.token.localComputationId
    }
    service.deleteComputation(deleteComputationRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getComputationToken(
          getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `updateComputationDetails updates ComputationDetails`() = runBlocking {
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token

    val updatedComputationDetails =
      createdToken.computationDetails.copy {
        liquidLegionsV2 =
          liquidLegionsV2.copy {
            participant.clear()
            participant += computationParticipant { duchyId = "worker1" }
          }
      }
    val response =
      service.updateComputationDetails(
        updateComputationDetailsRequest {
          token = createdToken
          details = updatedComputationDetails
        }
      )

    assertThat(response.token)
      .isEqualTo(createdToken.copy { computationDetails = updatedComputationDetails })
    assertThat(
        service
          .getComputationToken(
            getComputationTokenRequest { globalComputationId = createdToken.globalComputationId }
          )
          .token
      )
      .isEqualTo(response.token)
  }

  @Test
  fun `updateComputationDetails throws IllegalArgumentException when updating protocol`() =
    runBlocking {
      val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val exception =
        assertFailsWith<IllegalArgumentException> {
          service.updateComputationDetails(
            updateComputationDetailsRequest {
              token = createComputationResp.token
              details = ComputationDetails.getDefaultInstance()
            }
          )
        }

      assertThat(exception.message).contains("type")
    }

  @Test
  fun `advanceComputationStage returns token with updated stage`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val nextStage = computationStage {
      liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
    }
    val advanceComputationStageResp =
      service.advanceComputationStage(
        advanceComputationStageRequest {
          token = claimWorkResponse.token
          nextComputationStage = nextStage
          afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
        }
      )

    assertThat(advanceComputationStageResp.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(claimWorkResponse.token.copy { computationStage = nextStage })
  }

  @Test
  fun `advanceComputationStage throws IllegalArgumentException when transition stage is invalid`() =
    runBlocking {
      val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val nextStage = computationStage { liquidLegionsSketchAggregationV2 = Stage.SETUP_PHASE }
      val exception =
        assertFailsWith<IllegalArgumentException> {
          service.advanceComputationStage(
            advanceComputationStageRequest {
              token = createComputationResp.token
              nextComputationStage = nextStage
              afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
            }
          )
        }

      assertThat(exception.message).contains("Invalid stage transition")
    }

  @Test
  fun `advanceComputationStage throws IllegalStateException when afterTransition is invalid`() =
    runBlocking {
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

      val nextStage = computationStage {
        liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          service.advanceComputationStage(
            advanceComputationStageRequest {
              token = claimWorkResponse.token
              nextComputationStage = nextStage
              afterTransition = AfterTransition.UNSPECIFIED
            }
          )
        }

      assertThat(exception.message)
        .contains("Unsupported AdvanceComputationStageRequest.AfterTransition")
    }

  @Test
  fun `advanceComputationStage throws ABORTED when token is not the latest`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    clock.tickSeconds("1_second_later", 1)
    service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val nextStage = computationStage {
      liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.advanceComputationStage(
          advanceComputationStageRequest {
            token = createComputationResp.token
            nextComputationStage = nextStage
            afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
          }
        )
      }

    assertThat(exception.message).ignoringCase().contains("version")
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `advanceComputationStage enqueues the computation when afterTransition is ADD_UNCLAIMED_TO_QUEUE`() =
    runBlocking {
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

      val nextStage = computationStage {
        liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
      }
      val advanceComputationStageResp =
        service.advanceComputationStage(
          advanceComputationStageRequest {
            token = claimWorkResponse.token
            nextComputationStage = nextStage
            afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          }
        )

      assertThat(advanceComputationStageResp.token.attempt).isEqualTo(0)
      assertThat(advanceComputationStageResp.token.lockOwner).isEmpty()
      assertThat(advanceComputationStageResp.token.lockExpirationTime)
        .isEqualTo(clock.last().toProtoTime())
    }

  @Test
  fun `advanceComputationStage releases the computation lock when afterTransition is DO_NOT_ADD_TO_QUEUE`() =
    runBlocking {
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

      val nextStage = computationStage {
        liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
      }
      val advanceComputationStageResp =
        service.advanceComputationStage(
          advanceComputationStageRequest {
            token = claimWorkResponse.token
            nextComputationStage = nextStage
            afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
          }
        )

      assertThat(advanceComputationStageResp.token.lockOwner).isEmpty()
      assertThat(advanceComputationStageResp.token.lockExpirationTime).isEqualToDefaultInstance()
    }

  @Test
  fun `advanceComputationStage throws when output blobs are not fulfilled`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)
    val advanceResp =
      service.advanceComputationStage(
        advanceComputationStageRequest {
          token = claimWorkResponse.token
          nextComputationStage = computationStage {
            liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
          }
          afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
          outputBlobs = 2
        }
      )

    val nextStage = computationStage { liquidLegionsSketchAggregationV2 = Stage.CONFIRMATION_PHASE }
    val exception =
      assertFailsWith<IllegalStateException> {
        service.advanceComputationStage(
          advanceComputationStageRequest {
            token = advanceResp.token
            nextComputationStage = nextStage
            afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
          }
        )
      }

    assertThat(exception.message).contains("written")
  }

  @Test
  fun `finishComputation returns computation in terminal stage`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    clock.tickSeconds("finish", 1)

    val finishComputationRequest = finishComputationRequest {
      token = createComputationResp.token
      endingComputationStage = Stage.COMPLETE.toProtocolStage()
      reason = ComputationDetails.CompletedReason.SUCCEEDED
    }
    val finishComputationResponse = service.finishComputation(finishComputationRequest)

    val expectedComputationToken =
      createComputationResp.token.copy {
        computationStage = Stage.COMPLETE.toProtocolStage()
        computationDetails =
          this.computationDetails.copy {
            endingState = ComputationDetails.CompletedReason.SUCCEEDED
          }
        clearLockExpirationTime()
      }
    assertThat(finishComputationResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedComputationToken)
  }

  @Test
  fun `finishComputation throws IllegalStateException when complete reason is UNSPECIFIED`() =
    runBlocking {
      val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val finishComputationRequest = finishComputationRequest {
        token = createComputationResp.token
        endingComputationStage = Stage.COMPLETE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.UNSPECIFIED
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          service.finishComputation(finishComputationRequest)
        }

      assertThat(exception.message).contains("Unknown CompletedReason")
    }

  @Test
  fun `finishComputation throws IllegalStateException when terminal stage is invalid`() =
    runBlocking {
      val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val finishComputationRequest = finishComputationRequest {
        token = createComputationResp.token
        endingComputationStage = Stage.SETUP_PHASE.toProtocolStage()
        reason = ComputationDetails.CompletedReason.SUCCEEDED
      }
      val exception =
        assertFailsWith<IllegalArgumentException> {
          service.finishComputation(finishComputationRequest)
        }

      assertThat(exception.message).contains("Invalid terminal stage of computation")
    }

  @Test
  fun `claimWork returns empty response when no computation to be claimed`() = runBlocking {
    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    assertThat(claimWorkResponse).isEqualTo(ClaimWorkResponse.getDefaultInstance())
  }

  @Test
  fun `claimWork throws INVALID_ARGUMENT when owner is not specified`() = runBlocking {
    val claimWorkRequest = DEFAULT_CLAIM_WORK_REQUEST.copy { clearOwner() }

    val exception = assertFailsWith<StatusRuntimeException> { service.claimWork(claimWorkRequest) }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
    assertThat(exception.message).contains("owner")
  }

  @Test
  fun `claimWork updates computation lock`(): Unit = runBlocking {
    val token: ComputationToken =
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    clock.tickSeconds("claimTime", 1)
    val claimTime: Instant = clock.last()
    val lockDuration = Duration.ofMinutes(10)
    reset(mockComputationLogEntriesService)

    val request: ClaimWorkRequest =
      DEFAULT_CLAIM_WORK_REQUEST.copy { this.lockDuration = lockDuration.toProtoDuration() }
    val response: ClaimWorkResponse = service.claimWork(request)

    assertThat(response.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(
        token.copy {
          attempt = 1
          lockOwner = request.owner
          lockExpirationTime = claimTime.plus(lockDuration).toProtoTime()
        }
      )
    assertThat(response.token.version).isGreaterThan(token.version)
    assertThat(
        service
          .getComputationToken(
            getComputationTokenRequest { globalComputationId = response.token.globalComputationId }
          )
          .token
      )
      .isEqualTo(response.token)
    verifyProtoArgument(
        mockComputationLogEntriesService,
        ComputationLogEntriesCoroutineImplBase::createComputationLogEntry,
      )
      .ignoringFieldDescriptors(
        ComputationLogEntry.getDescriptor()
          .findFieldByNumber(ComputationLogEntry.LOG_MESSAGE_FIELD_NUMBER)
      )
      .isEqualTo(
        createComputationLogEntryRequest {
          parent = "computations/${token.globalComputationId}/participants/$DUCHY_ID"
          computationLogEntry = computationLogEntry {
            participantChildReferenceId = request.owner
            stageAttempt = stageAttempt {
              stage = Stage.INITIALIZATION_PHASE_VALUE
              stageName = Stage.INITIALIZATION_PHASE.name
              attemptNumber = response.token.attempt.toLong()
              stageStartTime = claimTime.toProtoTime()
            }
          }
        }
      )
  }

  @Test
  fun `claimWork updates computation lock when previous lock expired`() = runBlocking {
    val createdToken = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST).token
    service.claimWork(
      DEFAULT_CLAIM_WORK_REQUEST.copy { this.lockDuration = Durations.fromSeconds(1) }
    )
    clock.tickSeconds("lock-expired", 1)
    val updateTime = clock.last()
    val lockDuration = Duration.ofMinutes(10)

    val response =
      service.claimWork(
        DEFAULT_CLAIM_WORK_REQUEST.copy { this.lockDuration = lockDuration.toProtoDuration() }
      )

    assertThat(response.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(
        createdToken.copy {
          attempt = 2
          lockOwner = DEFAULT_OWNER
          lockExpirationTime = updateTime.plus(lockDuration).toProtoTime()
        }
      )
    assertThat(
        service
          .getComputationToken(
            getComputationTokenRequest { globalComputationId = response.token.globalComputationId }
          )
          .token
      )
      .isEqualTo(response.token)
  }

  @Test
  fun `claimWork returns empty response when all computations are claimed`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    assertThat(claimWorkResponse).isEqualTo(ClaimWorkResponse.getDefaultInstance())
  }

  @Test
  fun `claimWork returns computation of prioritized stage`() = runBlocking {
    service
      .createComputation(
        DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
          globalComputationId = "1"
          requisitions.clear()
        }
      )
      .token

    var computation1 =
      service
        .claimWork(
          DEFAULT_CLAIM_WORK_REQUEST.copy {
            this.lockDuration = Durations.fromSeconds(1)
            prioritizedStages += Stage.INITIALIZATION_PHASE.toProtocolStage()
          }
        )
        .token

    computation1 =
      service
        .advanceComputationStage(
          advanceComputationStageRequest {
            token = computation1
            nextComputationStage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
            // To simplify the test, assume WAIT_REQUISITIONS_AND_KEY_SET is claimable.
            afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          }
        )
        .token

    // create a new computation with prioritized stage.
    val computation2 =
      service
        .createComputation(
          DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
            globalComputationId = "2"
            requisitions.clear()
          }
        )
        .token

    val response =
      service.claimWork(
        DEFAULT_CLAIM_WORK_REQUEST.copy {
          this.lockDuration = Durations.fromSeconds(1)
          prioritizedStages += Stage.INITIALIZATION_PHASE.toProtocolStage()
        }
      )

    assertThat(response.token.globalComputationId).isEqualTo(computation2.globalComputationId)
  }

  @Test
  fun `getComputationIds returns empty response when no matching computations`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val getComputationIdsRequest = getComputationIdsRequest {
      stages += Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
    }
    val getComputationIdsResponse = service.getComputationIds(getComputationIdsRequest)

    assertThat(getComputationIdsResponse).isEqualTo(GetComputationIdsResponse.getDefaultInstance())
  }

  @Test
  fun `getComputationIds returns all IDs in given stages`(): Unit = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val secondComputationId = "5678"
    service.createComputation(
      DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
        globalComputationId = secondComputationId
        requisitions[0] = requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
      }
    )
    service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val getComputationIdsRequest = getComputationIdsRequest {
      stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
      stages += Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
    }
    val getComputationIdsResponse = service.getComputationIds(getComputationIdsRequest)

    assertThat(getComputationIdsResponse.globalIdsList)
      .containsExactly(GLOBAL_COMPUTATION_ID, secondComputationId)
  }

  @Test
  fun `recordOutputBlobPath returns token with updated blob path`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)
    val advanceComputationStageResp =
      service.advanceComputationStage(
        advanceComputationStageRequest {
          token = claimWorkResponse.token
          nextComputationStage = computationStage {
            liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
          }
          afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
          outputBlobs = 2
        }
      )

    var latestToken = advanceComputationStageResp.token
    var expectedToken = latestToken
    val blobPathTemplate = "/path/to/output/blob_"
    advanceComputationStageResp.token.blobsList.forEach { blobMetadata ->
      val blobId = blobMetadata.blobId
      val blobPath = blobPathTemplate + blobId
      val recordOutputBlobPathRequest = recordOutputBlobPathRequest {
        token = latestToken
        this.outputBlobId = blobId
        this.blobPath = blobPath
      }
      val recordOutputBlobPathResponse = service.recordOutputBlobPath(recordOutputBlobPathRequest)

      expectedToken =
        expectedToken.copy {
          blobs[blobId.toInt()] = blobs[blobId.toInt()].copy { path = blobPath }
        }
      assertThat(recordOutputBlobPathResponse.token)
        .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
        .isEqualTo(expectedToken)
      latestToken = recordOutputBlobPathResponse.token
    }
  }

  @Test
  fun `recordOutputBlobPath throws IllegalStateException when blobId does not exist`(): Unit =
    runBlocking {
      val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val computationToken = createComputationResponse.token
      val blobId = 0L
      val blobPath = "blobPath"
      val recordOutputBlobPathRequest = recordOutputBlobPathRequest {
        token = computationToken
        this.outputBlobId = blobId
        this.blobPath = blobPath
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          service.recordOutputBlobPath(recordOutputBlobPathRequest)
        }

      assertThat(exception.message).contains("No ComputationBlobReferences row")
    }

  @Test
  fun `recordOutputBlobPath throws IllegalStateException when blob is not OUTPUT type`(): Unit =
    runBlocking {
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)
      val advanceComputationStageResp =
        service.advanceComputationStage(
          advanceComputationStageRequest {
            token = claimWorkResponse.token
            nextComputationStage = computationStage {
              liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
            }
            afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
            inputBlobs += "input_blob"
          }
        )

      val recordOutputBlobPathRequest = recordOutputBlobPathRequest {
        token = advanceComputationStageResp.token
        this.outputBlobId = advanceComputationStageResp.token.blobsList[0].blobId
        this.blobPath = "blob_path"
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          service.recordOutputBlobPath(recordOutputBlobPathRequest)
        }

      assertThat(exception.message).ignoringCase().contains("input")
    }

  @Test
  fun `enqueueComputation returns empty response`() = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val enqueueComputationRequest = enqueueComputationRequest {
      token = createComputationResponse.token
      delaySecond = 1
    }
    val enqueueComputationResponse = service.enqueueComputation(enqueueComputationRequest)

    assertThat(enqueueComputationResponse)
      .isEqualTo(EnqueueComputationResponse.getDefaultInstance())
  }

  @Test
  fun `enqueueComputation throws INVALID_ARGUMENT when delay seconds is negative`(): Unit =
    runBlocking {
      val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val enqueueComputationRequest = enqueueComputationRequest {
        token = createComputationResponse.token
        delaySecond = -1
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.enqueueComputation(enqueueComputationRequest)
        }

      assertThat(exception.message).contains("should be non-negative")
    }

  @Test
  fun `recordRequisitionFulfillment with blobPath returns updated token`() = runBlocking {
    val createComputationResponse =
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST.copy {})

    val blobPath = "/path/to/requisition/blob"
    val recordRequisitionFulfillmentRequest = recordRequisitionFulfillmentRequest {
      token = createComputationResponse.token
      key = DEFAULT_REQUISITION_ENTRY.key
      this.blobPath = blobPath
      publicApiVersion = Version.V2_ALPHA.string
    }
    val recordRequisitionBlobResponse =
      service.recordRequisitionFulfillment(recordRequisitionFulfillmentRequest)

    val expectedToken =
      createComputationResponse.token.copy {
        requisitions[0] =
          requisitions[0].copy {
            path = blobPath
            details = details.copy { publicApiVersion = Version.V2_ALPHA.string }
          }
      }
    assertThat(recordRequisitionBlobResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedToken)
  }

  @Test
  fun `recordRequisitionFulfillment with blobPath and seed returns updated token`() = runBlocking {
    val createComputationResponse =
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST.copy {})

    val blobPath = "/path/to/requisition/blob"
    val secretSeed = "encrypted seed".toByteStringUtf8()
    val registerCount = 100L
    val dataProviderCertificate = "dataProviders/1/certificates/2"
    val recordRequisitionFulfillmentRequest = recordRequisitionFulfillmentRequest {
      token = createComputationResponse.token
      key = DEFAULT_REQUISITION_ENTRY.key
      this.blobPath = blobPath
      publicApiVersion = Version.V2_ALPHA.string
      protocolDetails =
        RequisitionDetailsKt.requisitionProtocol {
          honestMajorityShareShuffle = honestMajorityShareShuffle {
            this.secretSeedCiphertext = secretSeed
            this.registerCount = registerCount
            this.dataProviderCertificate = dataProviderCertificate
          }
        }
    }
    val recordRequisitionBlobResponse =
      service.recordRequisitionFulfillment(recordRequisitionFulfillmentRequest)

    val expectedToken =
      createComputationResponse.token.copy {
        requisitions[0] =
          requisitions[0].copy {
            path = blobPath
            details =
              details.copy {
                publicApiVersion = Version.V2_ALPHA.string
                protocol =
                  RequisitionDetailsKt.requisitionProtocol {
                    honestMajorityShareShuffle = honestMajorityShareShuffle {
                      this.secretSeedCiphertext = secretSeed
                      this.registerCount = registerCount
                      this.dataProviderCertificate = dataProviderCertificate
                    }
                  }
              }
          }
      }
    assertThat(recordRequisitionBlobResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedToken)
  }

  @Test
  fun `recordRequisitionFulfillment throws IllegalArgumentException when path is blank`(): Unit =
    runBlocking {
      val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val blankBlobPath = ""
      val recordRequisitionFulfillmentRequest = recordRequisitionFulfillmentRequest {
        token = createComputationResponse.token
        key = DEFAULT_REQUISITION_ENTRY.key
        this.blobPath = blankBlobPath
      }
      val exception =
        assertFailsWith<IllegalArgumentException> {
          service.recordRequisitionFulfillment(recordRequisitionFulfillmentRequest)
        }

      assertThat(exception.message).contains("Cannot insert blank path to blob")
    }

  @Test
  fun `recordRequisitionFulfillment throws IllegalStateException when requisition does not exist`():
    Unit = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val blobPath = "/path/to/requisition/blob"
    val recordRequisitionFulfillmentRequest = recordRequisitionFulfillmentRequest {
      token = createComputationResponse.token
      key = externalRequisitionKey {
        externalRequisitionId = "dne_external_requsition_id"
        requisitionFingerprint = "dne_finger_print".toByteStringUtf8()
      }
      this.blobPath = blobPath
      publicApiVersion = Version.V2_ALPHA.string
    }
    val exception =
      assertFailsWith<IllegalStateException> {
        service.recordRequisitionFulfillment(recordRequisitionFulfillmentRequest)
      }

    assertThat(exception.message).contains("found")
  }
}
