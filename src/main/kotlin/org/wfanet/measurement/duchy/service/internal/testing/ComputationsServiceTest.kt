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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toByteString
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
  protected abstract fun newService(): T

  @Before
  fun initService() {
    service = newService()
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

    // TODO(world-federation-of-advertisers/cross-media-measurement#889): deleteComputation should throw
    // NOT_FOUND exception rather than return empty response.
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
  fun `purgeComputations does not delete Computations when force is false`() =
    runBlocking {
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
    val claimWorkRequest = claimWorkRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
    }
    val claimWorkResponse = service.claimWork(claimWorkRequest)
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
      DEFAULT_CREATE_COMPUTATION_RESP_TOKEN.copy {
        computationDetails = updatedComputationDetails
      }
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
    val claimWorkResponse =
      service.claimWork(
        claimWorkRequest {
          computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        }
      )

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
      val claimWorkResponse =
        service.claimWork(
          claimWorkRequest {
            computationType =
              ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
          }
        )

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
      service.claimWork(
        claimWorkRequest {
          computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        }
      )

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
}
