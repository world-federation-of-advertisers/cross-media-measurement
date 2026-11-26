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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.time.Clock
import java.time.Duration
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.trustee.TrusTeeRequisitionDecryptor
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.PopulationSpecKt
import org.wfanet.measurement.internal.duchy.RequisitionDetails.RequisitionProtocol.TrusTee.DataFormat
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.populationSpec
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2Kt
import org.wfanet.measurement.internal.duchy.query
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.internal.duchy.vectorSet
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest

@RunWith(JUnit4::class)
class TrusTeeV2MillTest {
  private val mockSystemComputations: SystemComputationsCoroutineImplBase = mockService()
  private val mockComputationParticipants: ComputationParticipantsCoroutineImplBase = mockService()
  private val mockComputationLogEntries: ComputationLogEntriesCoroutineImplBase = mockService()
  private val mockComputationStats: ComputationStatsCoroutineImplBase = mockService()

  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var requisitionStore: RequisitionStore

  private val tempDirectory = TemporaryFolder()

  private val grpcTestServerRule =
    GrpcTestServerRule(defaultServiceConfig = MillBase.SERVICE_CONFIG) {
      DuchyInfo.setForTest(setOf(DUCHY_ID))

      val storageClient = FileSystemStorageClient(tempDirectory.root)
      requisitionStore = RequisitionStore(storageClient)
      val computationStore = ComputationStore(storageClient)
      computationDataClients =
        ComputationDataClients.forTesting(
          ComputationsCoroutineStub(channel),
          computationStore,
          requisitionStore,
        )
      addService(mockSystemComputations)
      addService(mockComputationLogEntries)
      addService(mockComputationParticipants)
      addService(mockComputationStats)
      addService(
        ComputationsService(
          fakeComputationDb,
          ComputationLogEntriesCoroutineStub(channel),
          computationStore,
          requisitionStore,
          DUCHY_ID,
          clock = Clock.systemUTC(),
        )
      )
    }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private fun createMill(): TrusTeeV2Mill {
    val channel = grpcTestServerRule.channel
    return TrusTeeV2Mill(
      millId = MILL_ID,
      duchyId = DUCHY_ID,
      signingKey = DUCHY_SIGNING_KEY,
      consentSignalCert = Certificate(DUCHY_CERT_NAME, DUCHY_SIGNING_CERT),
      dataClients = computationDataClients,
      systemComputationParticipantsClient = ComputationParticipantsCoroutineStub(channel),
      systemComputationsClient = SystemComputationsCoroutineStub(channel),
      systemComputationLogEntriesClient = ComputationLogEntriesCoroutineStub(channel),
      computationStatsClient = ComputationStatsCoroutineStub(channel),
      workLockDuration = Duration.ofMinutes(5),
      // Only plaintext vectors are read, so no KMS client is used.
      requisitionDecryptor =
        TrusTeeRequisitionDecryptor(
          gcloudKmsClientFactory = mock(),
          gcloudToAwsKmsClientFactory = mock(),
          confidentialSpaceToAwsKmsClientFactory = mock(),
          attestationTokenPath = Path("attestation/token/path"),
        ),
      privacyParams = CellPrivacyParams(NoiseMechanism.NONE, resultMinimumThresholds = null),
      fallbackEventDescriptor = null,
    )
  }

  /** Adds a computation at [stage] whose requisitions are fulfilled with [vectors]. */
  private suspend fun addComputation(
    stage: Stage,
    vectors: List<ByteArray>,
    requisitions: List<RequisitionMetadata> =
      vectors.indices.map { requisition(it, vectorIndex = it) },
  ) {
    for ((requisition, vector) in requisitions.zip(vectors)) {
      requisitionStore.write(
        RequisitionBlobContext(GLOBAL_ID, requisition.externalKey.externalRequisitionId),
        vector.toByteString(),
      )
    }
    fakeComputationDb.addComputation(
      LOCAL_ID,
      stage.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = requisitions,
    )
  }

  @Test
  fun `initialization sends requisition params to the Kingdom and waits to start`(): Unit =
    runBlocking {
      val participant = computationParticipant {
        name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ID).toName()
        etag = "entity tag"
        state = ComputationParticipant.State.CREATED
      }
      whenever(mockComputationParticipants.getComputationParticipant(any())).thenReturn(participant)
      addComputation(Stage.INITIALIZED, VECTORS)

      createMill().claimAndProcessWork()

      assertThat(fakeComputationDb[LOCAL_ID]!!.computationStage)
        .isEqualTo(Stage.WAIT_TO_START.toProtocolStage())
      verifyProtoArgument(
          mockComputationParticipants,
          ComputationParticipantsCoroutineImplBase::setParticipantRequisitionParams,
        )
        .isEqualTo(
          setParticipantRequisitionParamsRequest {
            name = participant.name
            etag = participant.etag
            requisitionParams =
              ComputationParticipantKt.requisitionParams {
                duchyCertificate = DUCHY_CERT_NAME
                trusTee = ComputationParticipant.RequisitionParams.TrusTee.getDefaultInstance()
              }
          }
        )
    }

  @Test
  fun `computing sets the multimeasurement result and completes`(): Unit = runBlocking {
    addComputation(Stage.COMPUTING, VECTORS)

    createMill().claimAndProcessWork()

    val token = fakeComputationDb[LOCAL_ID]!!
    assertThat(token.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(token.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.SUCCEEDED)
    val requestCaptor = argumentCaptor<SetComputationResultRequest>()
    verify(mockSystemComputations).setComputationResult(requestCaptor.capture())
    val request: SetComputationResultRequest = requestCaptor.firstValue
    assertThat(request.name).isEqualTo("computations/$GLOBAL_ID")
    assertThat(request.publicApiVersion).isEqualTo(PUBLIC_API_VERSION)
    val queryResult =
      Measurement.Result.parseFrom(request.result).multimeasurementResult.queryResultsList.single()
    assertThat(queryResult.queryId).isEqualTo(QUERY_ID)
    val cell = queryResult.cellsList.single()
    assertThat(cell.reach).isEqualTo(EXPECTED_REACH)
    assertThat(cell.impressions).isEqualTo(EXPECTED_IMPRESSIONS)
  }

  @Test
  fun `computing fails when a requisition has no vector index`(): Unit = runBlocking {
    addComputation(
      Stage.COMPUTING,
      VECTORS,
      requisitions = listOf(requisition(0, vectorIndex = 0), requisition(1, vectorIndex = null)),
    )

    createMill().claimAndProcessWork()

    assertFailed()
  }

  @Test
  fun `computing fails when a vector does not match the population`(): Unit = runBlocking {
    addComputation(Stage.COMPUTING, VECTORS.map { it.copyOf(it.size - 1) })

    createMill().claimAndProcessWork()

    assertFailed()
  }

  private suspend fun assertFailed() {
    val token = fakeComputationDb[LOCAL_ID]!!
    assertThat(token.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(token.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  companion object {
    private const val PUBLIC_API_VERSION = "v2alpha"
    private const val MILL_ID = "test-trustee-v2-mill"
    private const val DUCHY_ID = "aggregator"
    private const val LOCAL_ID = 1234L
    private const val GLOBAL_ID = LOCAL_ID.toString()
    private const val MEASUREMENT_NAME = "measurementConsumers/123/measurements/$GLOBAL_ID"
    private const val DUCHY_CERT_NAME = "cert 1"
    private const val QUERY_ID = "reach"

    private val DUCHY_SIGNING_CERT =
      readCertificate(TestData.FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString())
    private val DUCHY_SIGNING_KEY =
      SigningKeyHandle(
        DUCHY_SIGNING_CERT,
        readPrivateKey(
          TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString(),
          DUCHY_SIGNING_CERT.publicKey.algorithm,
        ),
      )

    private val DATA_PROVIDERS = listOf("edp-a", "edp-b")

    /** One base vector per [DATA_PROVIDERS] entry, over all 8 VIDs of [POPULATION_SPEC]. */
    private val VECTORS: List<ByteArray> =
      listOf(byteArrayOf(1, 0, 1, 0, 0, 0, 0, 0), byteArrayOf(0, 0, 1, 1, 0, 0, 0, 0))
    // Positions 0, 2 and 3 are reached, with 1 + 2 + 1 impressions.
    private const val EXPECTED_REACH = 3L
    private const val EXPECTED_IMPRESSIONS = 4L

    private val POPULATION_SPEC = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          attributes +=
            ProtoAny.pack(person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 }).toByteString()
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1
              endVidInclusive = 4
            }
        }
      subpopulations +=
        PopulationSpecKt.subPopulation {
          attributes +=
            ProtoAny.pack(person { ageGroup = Person.AgeGroup.YEARS_35_TO_54 }).toByteString()
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 5
              endVidInclusive = 8
            }
        }
    }

    private val COMPUTATION_DETAILS = computationDetails {
      kingdomComputation =
        ComputationDetailsKt.kingdomComputationDetails {
          publicApiVersion = PUBLIC_API_VERSION
          measurement = MEASUREMENT_NAME
          participantCount = 1
        }
      trusTeeV2 =
        TrusTeeV2Kt.computationDetails {
          role = RoleInComputation.AGGREGATOR
          queries += query {
            id = QUERY_ID
            summands = vectorSet { baseVectors += listOf(0, 1) }
            maxFrequency = 2
          }
          populationSpec = POPULATION_SPEC
          eventDescriptor =
            TrusTeeV2Kt.eventDescriptor {
              descriptorSet = ProtoReflection.buildFileDescriptorSet(TestEvent.getDescriptor())
              eventMessageType = TestEvent.getDescriptor().fullName
            }
          vidSamplingInterval =
            TrusTeeV2Kt.vidSamplingInterval {
              start = 0f
              width = 1f
            }
        }
    }

    /** Returns a plaintext requisition of [DATA_PROVIDERS] entry [index], at [vectorIndex]. */
    private fun requisition(index: Int, vectorIndex: Int?): RequisitionMetadata {
      val requisitionId = "requisition-$index"
      return requisitionMetadata {
        externalKey = externalRequisitionKey {
          externalRequisitionId = requisitionId
          requisitionFingerprint = ByteString.copyFromUtf8(requisitionId)
        }
        details = requisitionDetails {
          if (vectorIndex != null) {
            this.vectorIndex = vectorIndex
          }
          protocol =
            RequisitionDetailsKt.requisitionProtocol {
              trusTeeV2 =
                RequisitionProtocolKt.trusTeeV2 {
                  trusTee =
                    RequisitionProtocolKt.trusTee { dataFormat = DataFormat.FREQUENCY_VECTOR }
                  dataProviderResourceId = DATA_PROVIDERS[index]
                }
            }
        }
        path = RequisitionBlobContext(GLOBAL_ID, requisitionId).blobKey
      }
    }
  }
}
