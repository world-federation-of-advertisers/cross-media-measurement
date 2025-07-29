// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal.computations

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.honestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.deleteComputationRequest
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.recordRequisitionFulfillmentRequest
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionEntry
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest

private val AGGREGATOR_COMPUTATION_DETAILS =
  ComputationDetails.newBuilder()
    .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.AGGREGATOR } }
    .build()

private val NON_AGGREGATOR_COMPUTATION_DETAILS =
  ComputationDetails.newBuilder()
    .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.NON_AGGREGATOR } }
    .build()
private const val DUCHY_NAME = "BOHEMIA"
private const val COMPUTATION_BLOB_KEY_PREFIX = "computations"
private const val REQUISITION_BLOB_KEY_PREFIX = "requisitions"

@RunWith(JUnit4::class)
@ExperimentalCoroutinesApi
class ComputationsServiceTest {

  private val fakeDatabase = FakeComputationsDatabase()
  private val mockComputationLogEntriesService: ComputationLogEntriesCoroutineImplBase =
    mockService()

  private val tempDirectory = TemporaryFolder()
  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore

  val grpcTestServerRule = GrpcTestServerRule {
    storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    addService(mockComputationLogEntriesService)
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private val service: ComputationsService by lazy {
    ComputationsService(
      fakeDatabase,
      ComputationLogEntriesCoroutineStub(grpcTestServerRule.channel),
      computationStore,
      requisitionStore,
      DUCHY_NAME,
      clock = Clock.systemUTC(),
    )
  }

  @Test
  fun `get computation token`() = runBlocking {
    val id = "1234"
    val requisitionMetadata = requisitionMetadata {
      externalKey = externalRequisitionKey {
        externalRequisitionId = "1234"
        requisitionFingerprint = "A requisition fingerprint".toByteStringUtf8()
      }
    }
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(requisitionMetadata),
    )

    val expectedToken = computationToken {
      localComputationId = 1234
      globalComputationId = "1234"
      computationStage = computationStage {
        liquidLegionsSketchAggregationV2 =
          LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
      }
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS
      requisitions += requisitionMetadata
    }

    assertThat(service.getComputationToken(id.toGetTokenRequest()))
      .isEqualTo(expectedToken.toGetComputationTokenResponse())
    assertThat(service.getComputationToken(id.toGetTokenRequest()))
      .isEqualTo(expectedToken.toGetComputationTokenResponse())
  }

  @Test
  fun `update computationDetails successfully`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
    )
    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token
    val newComputationDetails =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = 123 }
        .build()
    val request =
      UpdateComputationDetailsRequest.newBuilder()
        .apply {
          token = tokenAtStart
          details = newComputationDetails
        }
        .build()

    assertThat(service.updateComputationDetails(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .apply {
            version = 1
            computationDetails = newComputationDetails
          }
          .build()
          .toUpdateComputationDetailsResponse()
      )
  }

  @Test
  fun `update computations details and requisition details`() = runBlocking {
    val id = "1234"
    val requisition1Key = externalRequisitionKey {
      externalRequisitionId = "1234"
      requisitionFingerprint = "A requisition fingerprint".toByteStringUtf8()
    }
    val requisition2Key = externalRequisitionKey {
      externalRequisitionId = "5678"
      requisitionFingerprint = "Another requisition fingerprint".toByteStringUtf8()
    }
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions =
        listOf(
          requisitionMetadata { externalKey = requisition1Key },
          requisitionMetadata { externalKey = requisition2Key },
        ),
    )
    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token
    val newComputationDetails =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = 123 }
        .build()
    val requisitionDetails1 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-1" }.build()
    val requisitionDetails2 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-2" }.build()
    val request = updateComputationDetailsRequest {
      token = tokenAtStart
      details = newComputationDetails
      requisitions += requisitionEntry {
        key = requisition1Key
        value = requisitionDetails1
      }
      requisitions += requisitionEntry {
        key = requisition2Key
        value = requisitionDetails2
      }
    }

    assertThat(service.updateComputationDetails(request))
      .isEqualTo(
        tokenAtStart
          .copy {
            version = 1
            computationDetails = newComputationDetails

            requisitions.clear()
            requisitions += requisitionMetadata {
              externalKey = requisition1Key
              details = requisitionDetails1
            }
            requisitions += requisitionMetadata {
              externalKey = requisition2Key
              details = requisitionDetails2
            }
          }
          .toUpdateComputationDetailsResponse()
      )
  }

  @Test
  fun `end failed computation`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
    )
    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token
    val request =
      FinishComputationRequest.newBuilder()
        .apply {
          token = tokenAtStart
          endingComputationStage = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()
          reason = ComputationDetails.CompletedReason.FAILED
        }
        .build()

    assertThat(service.finishComputation(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearStageSpecificDetails()
          .apply {
            version = 1
            computationStage = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()
            computationDetailsBuilder.endingState = ComputationDetails.CompletedReason.FAILED
          }
          .build()
          .toFinishComputationResponse()
      )

    verifyProtoArgument(
        mockComputationLogEntriesService,
        ComputationLogEntriesCoroutineImplBase::createComputationLogEntry,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateComputationLogEntryRequest.newBuilder()
          .apply {
            parent = "computations/$id/participants/$DUCHY_NAME"
            computationLogEntryBuilder.apply {
              logMessage = "Computation $id at stage COMPLETE, attempt 0"
              stageAttemptBuilder.apply {
                stage = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.number
                stageName = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.name
                attemptNumber = 0
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `write reference to output blob and advance stage`() = runBlocking {
    val id = "67890"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          newInputBlobMetadata(id = 0L, key = "an_input_blob"),
          newEmptyOutputBlobMetadata(id = 1L),
        ),
    )
    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token

    val tokenAfterRecordingBlob =
      service
        .recordOutputBlobPath(
          RecordOutputBlobPathRequest.newBuilder()
            .apply {
              token = tokenAtStart
              outputBlobId = 1L
              blobPath = "the_writen_output_blob"
            }
            .build()
        )
        .token

    val request =
      AdvanceComputationStageRequest.newBuilder()
        .apply {
          token = tokenAfterRecordingBlob
          nextComputationStage =
            LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
          addAllInputBlobs(listOf("inputs_to_new_stage"))
          outputBlobs = 1
          afterTransition = AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
        }
        .build()

    assertThat(service.advanceComputationStage(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearBlobs()
          .clearStageSpecificDetails()
          .apply {
            version = 2
            attempt = 1
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
                .toProtocolStage()
            addBlobs(newInputBlobMetadata(id = 0L, key = "inputs_to_new_stage"))
            addBlobs(newEmptyOutputBlobMetadata(id = 1L))
          }
          .build()
          .toAdvanceComputationStageResponse()
      )

    verifyProtoArgument(
        mockComputationLogEntriesService,
        ComputationLogEntriesCoroutineImplBase::createComputationLogEntry,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateComputationLogEntryRequest.newBuilder()
          .apply {
            parent = "computations/$id/participants/$DUCHY_NAME"
            computationLogEntryBuilder.apply {
              logMessage = "Computation $id at stage WAIT_EXECUTION_PHASE_TWO_INPUTS, attempt 0"
              stageAttemptBuilder.apply {
                stage =
                  LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.number
                stageName =
                  LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.name
                attemptNumber = 0
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `get computation ids`() = runBlocking {
    val blindId = "67890"
    val completedId = "12341"
    val decryptId = "4342242"
    fakeDatabase.addComputation(
      globalId = blindId,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
    )
    fakeDatabase.addComputation(
      completedId,
      LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage(),
      NON_AGGREGATOR_COMPUTATION_DETAILS,
      listOf(),
    )
    fakeDatabase.addComputation(
      decryptId,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE.toProtocolStage(),
      NON_AGGREGATOR_COMPUTATION_DETAILS,
      listOf(),
    )
    val getIdsInMillStagesRequest =
      GetComputationIdsRequest.newBuilder()
        .apply {
          addAllStages(
            setOf(
              LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
              LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE.toProtocolStage(),
            )
          )
        }
        .build()
    assertThat(service.getComputationIds(getIdsInMillStagesRequest))
      .isEqualTo(
        GetComputationIdsResponse.newBuilder()
          .apply { addAllGlobalIds(setOf(blindId, decryptId)) }
          .build()
      )
  }

  @Test
  fun `claim task`() = runBlocking {
    val unclaimed = "12345678"
    val claimed = "23456789"
    fakeDatabase.addComputation(
      globalId = unclaimed,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
    )
    val unclaimedAtStart = service.getComputationToken(unclaimed.toGetTokenRequest()).token
    fakeDatabase.addComputation(
      claimed,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      NON_AGGREGATOR_COMPUTATION_DETAILS,
      listOf(),
    )
    val owner = "TheOwner"
    fakeDatabase.claimedComputations[claimed] = owner
    val claimedAtStart = service.getComputationToken(claimed.toGetTokenRequest()).token
    val request =
      ClaimWorkRequest.newBuilder()
        .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)
        .setOwner(owner)
        .build()
    assertThat(service.claimWork(request))
      .isEqualTo(
        unclaimedAtStart.toBuilder().setVersion(1).setAttempt(1).build().toClaimWorkResponse()
      )
    assertThat(service.claimWork(request)).isEqualToDefaultInstance()
    assertThat(service.getComputationToken(claimed.toGetTokenRequest()))
      .isEqualTo(claimedAtStart.toGetComputationTokenResponse())

    verifyProtoArgument(
        mockComputationLogEntriesService,
        ComputationLogEntriesCoroutineImplBase::createComputationLogEntry,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateComputationLogEntryRequest.newBuilder()
          .apply {
            parent = "computations/$unclaimed/participants/$DUCHY_NAME"
            computationLogEntryBuilder.apply {
              logMessage = "Computation $unclaimed at stage EXECUTION_PHASE_ONE, attempt 1"
              stageAttemptBuilder.apply {
                stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.number
                stageName = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.name
                attemptNumber = 1
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `RecordRequisitionBlobPath records blob path`() = runBlocking {
    val id = "1234"
    val requisitionKey = externalRequisitionKey {
      externalRequisitionId = "1234"
      requisitionFingerprint = "A requisition fingerprint".toByteStringUtf8()
    }
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(requisitionMetadata { externalKey = requisitionKey }),
    )

    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token

    val request = recordRequisitionFulfillmentRequest {
      token = tokenAtStart
      key = requisitionKey
      blobPath = "this is a new path"
      publicApiVersion = "v2alpha"
    }

    assertThat(service.recordRequisitionFulfillment(request).token)
      .isEqualTo(
        tokenAtStart.copy {
          version = 1

          requisitions.clear()
          requisitions += requisitionMetadata {
            externalKey = requisitionKey
            path = "this is a new path"
            details = requisitionDetails { this.publicApiVersion = "v2alpha" }
          }
        }
      )
  }

  @Test
  fun `recordRequisitionFulfillment records blob path and seed`() = runBlocking {
    val id = "1234"
    val requisitionKey = externalRequisitionKey {
      externalRequisitionId = "1234"
      requisitionFingerprint = "A requisition fingerprint".toByteStringUtf8()
    }
    fakeDatabase.addComputation(
      globalId = id,
      stage = HonestMajorityShareShuffle.Stage.INITIALIZED.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(requisitionMetadata { externalKey = requisitionKey }),
    )

    val tokenAtStart = service.getComputationToken(id.toGetTokenRequest()).token

    val secretSeed = "signed secret seed".toByteStringUtf8()
    val request = recordRequisitionFulfillmentRequest {
      token = tokenAtStart
      key = requisitionKey
      blobPath = "this is a new path"
      publicApiVersion = "v2alpha"
      protocolDetails =
        RequisitionDetailsKt.requisitionProtocol {
          honestMajorityShareShuffle = honestMajorityShareShuffle {
            this.secretSeedCiphertext = secretSeed
            this.registerCount = 100L
            this.dataProviderCertificate = "dataProviders/123/certificates/100"
          }
        }
    }

    assertThat(service.recordRequisitionFulfillment(request).token)
      .isEqualTo(
        tokenAtStart.copy {
          version = 1
          requisitions.clear()
          requisitions += requisitionMetadata {
            externalKey = requisitionKey
            path = "this is a new path"
            details = requisitionDetails {
              this.publicApiVersion = "v2alpha"
              protocol =
                RequisitionDetailsKt.requisitionProtocol {
                  honestMajorityShareShuffle = honestMajorityShareShuffle {
                    this.secretSeedCiphertext = secretSeed
                    this.registerCount = 100L
                    this.dataProviderCertificate = "dataProviders/123/certificates/100"
                  }
                }
            }
          }
        }
      )
  }

  @Test
  fun `deleteComputation deletes Computation and blobs`() = runBlocking {
    val globalId = "65535"
    val requisitionBlobPath1 = "$REQUISITION_BLOB_KEY_PREFIX/${globalId}_1"
    val requisitionBlobPath2 = "$REQUISITION_BLOB_KEY_PREFIX/${globalId}_2"
    val computationBlobKey1 = "$COMPUTATION_BLOB_KEY_PREFIX/${globalId}_1"
    val computationBlobKey2 = "$COMPUTATION_BLOB_KEY_PREFIX/${globalId}_2"

    val requisition1Key = externalRequisitionKey {
      externalRequisitionId = "1234"
      requisitionFingerprint = "A requisition fingerprint".toByteStringUtf8()
    }
    val requisition2Key = externalRequisitionKey {
      externalRequisitionId = "5678"
      requisitionFingerprint = "Another requisition fingerprint".toByteStringUtf8()
    }
    fakeDatabase.addComputation(
      globalId = globalId,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions =
        listOf(
          requisitionMetadata {
            externalKey = requisition1Key
            path = requisitionBlobPath1
          },
          requisitionMetadata {
            externalKey = requisition2Key
            path = requisitionBlobPath2
          },
        ),
    )
    storageClient.writeBlob(requisitionBlobPath1, "requisition_1".toByteStringUtf8())
    storageClient.writeBlob(requisitionBlobPath2, "requisition_2".toByteStringUtf8())
    storageClient.writeBlob(computationBlobKey1, "computation_1".toByteStringUtf8())
    storageClient.writeBlob(computationBlobKey2, "computation_2".toByteStringUtf8())

    val localId = globalId.toLong()
    service.deleteComputation(deleteComputationRequest { localComputationId = localId })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getComputationToken(getComputationTokenRequest { globalComputationId = globalId })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)

    assertThat(storageClient.getBlob(requisitionBlobPath1)).isNull()
    assertThat(storageClient.getBlob(requisitionBlobPath2)).isNull()
    assertThat(storageClient.getBlob(computationBlobKey1)).isNull()
    assertThat(storageClient.getBlob(computationBlobKey2)).isNull()
  }
}
