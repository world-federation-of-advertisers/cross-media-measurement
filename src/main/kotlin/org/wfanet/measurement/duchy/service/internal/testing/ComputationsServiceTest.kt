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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.*
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest.AfterTransition
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt

@RunWith(JUnit4::class)
abstract class ComputationsServiceTest<T : ComputationsCoroutineImplBase> {
  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(clock: Clock): T

  private val clock = TestClockWithNamedInstants(Clock.systemUTC().instant().minusSeconds(10))

  @Before
  fun initService() {
    service = newService(clock)
  }

  companion object {
    private const val FAKE_ID = "1234"
    private const val GLOBAL_COMPUTATION_ID = FAKE_ID
    private const val REQUISITION_ID = FAKE_ID
    private const val REQUISITION_FINGERPRINT = "finger_print"
    private const val REQUISITION_NONCE = 1234L
    private val AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
      liquidLegionsV2 =
        LiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
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
    private const val DEFAULT_OWNER = "Saul Goodman"
    private val DEFAULT_CLAIM_WORK_REQUEST = claimWorkRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      owner = DEFAULT_OWNER
    }
  }

  @Test
  fun `createComputation returns response with token`() = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    assertThat(createComputationResponse.token.localComputationId).isNotEqualTo(0L)
    assertThat(createComputationResponse.token.version).isNotEqualTo(0L)
    assertThat(createComputationResponse.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER
      )
      .isEqualTo(DEFAULT_CREATE_COMPUTATION_RESP_TOKEN)
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
    val computation1 = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val computation2 =
      service.createComputation(
        DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
          globalComputationId = "5678"
          requisitions[0] =
            requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
        }
      )

    val currentTime = Clock.systemUTC().instant()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
          force = false
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList)
      .containsExactly(
        computation1.token.globalComputationId,
        computation2.token.globalComputationId
      )
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `purgeComputations does not delete Computations when force is false`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val currentTime = Clock.systemUTC().instant()
    service.purgeComputations(
      purgeComputationsRequest {
        updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
        stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
        force = false
      }
    )

    val getComputationResponse =
      service.getComputationToken(
        getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
      )

    assertThat(getComputationResponse.token.localComputationId).isNotEqualTo(0L)
    assertThat(getComputationResponse.token.version).isNotEqualTo(0L)
    assertThat(getComputationResponse.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER
      )
      .isEqualTo(DEFAULT_CREATE_COMPUTATION_RESP_TOKEN)
  }

  @Test
  fun `purgeComputations only returns the deleted count when force is true`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    service.createComputation(
      DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
        globalComputationId = "5678"
        requisitions[0] = requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
      }
    )

    val currentTime = Clock.systemUTC().instant()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
          force = true
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList).isEmpty()
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `getComputationToken throws NOT_FOUND when computation is purged`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val currentTime = Clock.systemUTC().instant()
    val purgeComputationsRequest = purgeComputationsRequest {
      updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
      stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
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
    // Creates two computations in INITIALIZATION_PHASE stage to be purged
    val computationInInitPhase1 =
      service.createComputation(
        DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
          globalComputationId = "3456"
          requisitions[0] =
            requisitions[0].copy { key = key.copy { externalRequisitionId = "3456" } }
        }
      )
    val computationInInitPhase2 =
      service.createComputation(
        DEFAULT_CREATE_COMPUTATION_REQUEST.copy {
          globalComputationId = "5678"
          requisitions[0] =
            requisitions[0].copy { key = key.copy { externalRequisitionId = "5678" } }
        }
      )

    val currentTime = Clock.systemUTC().instant()
    val purgeComputationsResp =
      service.purgeComputations(
        purgeComputationsRequest {
          updatedBefore = currentTime.plusSeconds(1000L).toProtoTime()
          stages += Stage.INITIALIZATION_PHASE.toProtocolStage()
          force = false
        }
      )

    assertThat(purgeComputationsResp.purgeSampleList)
      .containsExactly(
        computationInInitPhase1.token.globalComputationId,
        computationInInitPhase2.token.globalComputationId
      )
    assertThat(purgeComputationsResp.purgeCount).isEqualTo(2)
  }

  @Test
  fun `getComputationToken by global computation ID returns created computation`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val getComputationResponse =
      service.getComputationToken(
        getComputationTokenRequest { globalComputationId = GLOBAL_COMPUTATION_ID }
      )

    assertThat(getComputationResponse.token.localComputationId).isNotEqualTo(0L)
    assertThat(getComputationResponse.token.version).isNotEqualTo(0L)
    assertThat(getComputationResponse.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER
      )
      .isEqualTo(DEFAULT_CREATE_COMPUTATION_RESP_TOKEN)
  }

  @Test
  fun `getComputationToken by requisition key returns created computation`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val getComputationRequest = getComputationTokenRequest {
      requisitionKey = DEFAULT_REQUISITION_ENTRY.key
    }
    val getComputationResponse = service.getComputationToken(getComputationRequest)

    assertThat(getComputationResponse.token.localComputationId).isNotEqualTo(0L)
    assertThat(getComputationResponse.token.version).isNotEqualTo(0L)
    assertThat(getComputationResponse.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER
      )
      .isEqualTo(DEFAULT_CREATE_COMPUTATION_RESP_TOKEN)
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
  fun `updateComputationDetails returns token with updated computationDetails`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val updatedComputationDetails =
      AGGREGATOR_COMPUTATION_DETAILS.copy {
        liquidLegionsV2 =
          LiquidLegionsSketchAggregationV2Kt.computationDetails {
            role = LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR
          }
      }
    val updateComputationsDetailsResponse =
      service.updateComputationDetails(
        updateComputationDetailsRequest {
          token = createComputationResp.token
          details = updatedComputationDetails
        }
      )

    val expectedUpdatedToken =
      DEFAULT_CREATE_COMPUTATION_RESP_TOKEN.copy { computationDetails = updatedComputationDetails }
    assertThat(updateComputationsDetailsResponse.token)
      .ignoringFields(
        ComputationToken.LOCAL_COMPUTATION_ID_FIELD_NUMBER,
        ComputationToken.VERSION_FIELD_NUMBER
      )
      .isEqualTo(expectedUpdatedToken)
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
  fun `advanceComputationStage throws IllegalStateException when token is not the latest`() =
    runBlocking {
      val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      clock.tickSeconds("1_second_later", 1)
      service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

      val nextStage = computationStage {
        liquidLegionsSketchAggregationV2 = Stage.WAIT_REQUISITIONS_AND_KEY_SET
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          service.advanceComputationStage(
            advanceComputationStageRequest {
              token = createComputationResp.token
              nextComputationStage = nextStage
              afterTransition = AfterTransition.RETAIN_AND_EXTEND_LOCK
            }
          )
        }

      assertThat(exception.message).contains("editVersion mismatch")
    }

  @Test
  fun `finishComputation returns computation in terminal stage`() = runBlocking {
    val createComputationResp = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

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
  fun `claimWork throws INVALID_ARGUMENT when owner is blank`() = runBlocking {
    val claimWorkRequest = DEFAULT_CLAIM_WORK_REQUEST.copy { clearOwner() }

    val exception = assertFailsWith<StatusRuntimeException> { service.claimWork(claimWorkRequest) }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
    assertThat(exception.message).contains("owner should not be blank")
  }

  @Test
  fun `claimWork returns created computation when previous claim lock expired`() = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val lockDuration = Duration.ofSeconds(1)
    val claimWorkRequest =
      DEFAULT_CLAIM_WORK_REQUEST.copy { this.lockDuration = lockDuration.toProtoDuration() }
    service.claimWork(claimWorkRequest)
    clock.tickSeconds("after_expiration", lockDuration.seconds)
    val claimWorkResponse = service.claimWork(claimWorkRequest)

    val expectedClaimedComputationToken = createComputationResponse.token.copy { attempt = 2 }
    assertThat(claimWorkResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedClaimedComputationToken)
  }

  @Test
  fun `claimWork returns empty response when all computations are claimed`() = runBlocking {
    service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    assertThat(claimWorkResponse).isEqualTo(ClaimWorkResponse.getDefaultInstance())
  }

  @Test
  fun `claimWork returns claimed computation token`() = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val claimWorkResponse = service.claimWork(DEFAULT_CLAIM_WORK_REQUEST)

    val expectedClaimedComputationToken = createComputationResponse.token.copy { attempt = 1 }
    assertThat(claimWorkResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedClaimedComputationToken)
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
  fun `recordOutputBlobPath throws IllegalArgumentException when blob is not OUTPUT type`(): Unit =
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
        assertFailsWith<IllegalArgumentException> {
          service.recordOutputBlobPath(recordOutputBlobPathRequest)
        }

      assertThat(exception.message).contains("Cannot write to")
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
  fun `recordRequisitionBlobPath returns updated token`() = runBlocking {
    val createComputationResponse =
      service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST.copy {})

    val blobPath = "/path/to/requisition/blob"
    val recordRequisitionBlobPathRequest = recordRequisitionBlobPathRequest {
      token = createComputationResponse.token
      key = DEFAULT_REQUISITION_ENTRY.key
      this.blobPath = blobPath
    }
    val recordRequisitionBlobResponse =
      service.recordRequisitionBlobPath(recordRequisitionBlobPathRequest)

    val expectedToken =
      createComputationResponse.token.copy {
        requisitions[0] = requisitions[0].copy { path = blobPath }
      }
    assertThat(recordRequisitionBlobResponse.token)
      .ignoringFields(ComputationToken.VERSION_FIELD_NUMBER)
      .isEqualTo(expectedToken)
  }

  @Test
  fun `recordRequisitionBlobPath throws IllegalArgumentException when path is blank`(): Unit =
    runBlocking {
      val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

      val blankBlobPath = ""
      val recordRequisitionBlobPathRequest = recordRequisitionBlobPathRequest {
        token = createComputationResponse.token
        key = DEFAULT_REQUISITION_ENTRY.key
        this.blobPath = blankBlobPath
      }
      val exception =
        assertFailsWith<IllegalArgumentException> {
          service.recordRequisitionBlobPath(recordRequisitionBlobPathRequest)
        }

      assertThat(exception.message).contains("Cannot insert blank path to blob")
    }

  @Test
  fun `recordRequisitionBlobPath throws IllegalStateException when requisition does not exist`():
    Unit = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)

    val blobPath = "/path/to/requisition/blob"
    val recordRequisitionBlobPathRequest = recordRequisitionBlobPathRequest {
      token = createComputationResponse.token
      key = externalRequisitionKey {
        externalRequisitionId = "dne_external_requsition_id"
        requisitionFingerprint = "dne_finger_print".toByteStringUtf8()
      }
      this.blobPath = blobPath
    }
    val exception =
      assertFailsWith<IllegalStateException> {
        service.recordRequisitionBlobPath(recordRequisitionBlobPathRequest)
      }

    assertThat(exception.message).contains("No Computation found")
  }
}
