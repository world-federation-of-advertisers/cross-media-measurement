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

package org.wfanet.measurement.duchy.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest.Header
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.TrusTeeKt.envelopeEncryption
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.trusTee
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.encryptionKey
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.randomSeed
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.*
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage as HmssStage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage as Llv2Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage as TrusTeeStage
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.system.v1alpha.RequisitionKey as SystemRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.fulfillRequisitionRequest as systemFulfillRequisitionRequest

private fun buildSecretSeed(seed: String): EncryptedMessage {
  val randomSeed = randomSeed { data = seed.toByteStringUtf8() }
  val signedMessage = signedMessage {
    message = Any.pack(randomSeed)
    signature = "fake signature".toByteStringUtf8()
    signatureAlgorithmOid = "2.9999"
  }
  return encryptedMessage {
    ciphertext = signedMessage.toByteString()
    typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
  }
}

private const val DUCHY_ID = "worker1"
private const val COMPUTATION_ID = "xyz"
private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private val DATA_PROVIDER_API_ID = externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID)
private const val REQUISITION_API_ID = "abcd"
private const val NONCE = -3060866405677570814L // Hex: D5859E38A0A96502
private val NONCE_HASH =
  HexString("45FEAA185D434E0EB4747F547F0918AA5B8403DBBD7F90D6F0D8C536E2D620D7")
private val REQUISITION_FINGERPRINT = "A fingerprint".toByteStringUtf8()
private val TEST_REQUISITION_DATA = "some data".toByteStringUtf8()
private val TEST_REQUISITION_SEED = buildSecretSeed("secret seed")
private val REGISTER_COUNT = 100L
private val DATA_PROVIDER_CERTIFICATE = "dataProviders/123/certificates/2"
private val HEADER = header {
  name = CanonicalRequisitionKey(DATA_PROVIDER_API_ID, REQUISITION_API_ID).toName()
  requisitionFingerprint = REQUISITION_FINGERPRINT
  nonce = NONCE
}
private val HMSS_HEADER = header {
  name = CanonicalRequisitionKey(DATA_PROVIDER_API_ID, REQUISITION_API_ID).toName()
  requisitionFingerprint = REQUISITION_FINGERPRINT
  nonce = NONCE
  honestMajorityShareShuffle = honestMajorityShareShuffle {
    secretSeed = TEST_REQUISITION_SEED
    registerCount = REGISTER_COUNT
    dataProviderCertificate = DATA_PROVIDER_CERTIFICATE
  }
}

private val PLAIN_TRUS_TEE_HEADER = header {
  name = CanonicalRequisitionKey(DATA_PROVIDER_API_ID, REQUISITION_API_ID).toName()
  requisitionFingerprint = REQUISITION_FINGERPRINT
  nonce = NONCE
  trusTee = trusTee {
    dataFormat = Header.TrusTee.DataFormat.FREQUENCY_VECTOR
    populationSpecFingerprint = POPULATION_SPEC_FINGERPRINT
  }
}

private val ENCRYPTED_DEK_DATA = "encrypted dek".toByteStringUtf8()
private val KMS_KEK_URI = "some/uri/to/kms/kek"
private val WORKLOAD_IDENTITY_PROVIDER = "workload identity provider"
private val IMPERSONATED_SERVICE_ACCOUNT = "a service account for decryption"
private const val POPULATION_SPEC_FINGERPRINT = 100L
private val ENCRYPTED_TRUS_TEE_HEADER = header {
  name = CanonicalRequisitionKey(DATA_PROVIDER_API_ID, REQUISITION_API_ID).toName()
  requisitionFingerprint = REQUISITION_FINGERPRINT
  nonce = NONCE
  trusTee = trusTee {
    dataFormat = Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR
    envelopeEncryption = envelopeEncryption {
      encryptedDek = encryptionKey {
        format = EncryptionKey.Format.TINK_ENCRYPTED_KEYSET
        data = ENCRYPTED_DEK_DATA
      }
      kmsKekUri = KMS_KEK_URI
      workloadIdentityProvider = WORKLOAD_IDENTITY_PROVIDER
      impersonatedServiceAccount = IMPERSONATED_SERVICE_ACCOUNT
    }
    populationSpecFingerprint = POPULATION_SPEC_FINGERPRINT
  }
}

private val FULFILLED_RESPONSE = fulfillRequisitionResponse { state = Requisition.State.FULFILLED }
private val SYSTEM_REQUISITION_KEY = SystemRequisitionKey(COMPUTATION_ID, REQUISITION_API_ID)
private val REQUISITION_KEY = externalRequisitionKey {
  externalRequisitionId = REQUISITION_API_ID
  requisitionFingerprint = REQUISITION_FINGERPRINT
}
private val MEASUREMENT_SPEC = measurementSpec { nonceHashes += NONCE_HASH.bytes }
private val COMPUTATION_DETAILS = computationDetails {
  kingdomComputation = kingdomComputationDetails {
    publicApiVersion = Version.V2_ALPHA.string
    measurementSpec = MEASUREMENT_SPEC.toByteString()
  }
}
private val REQUISITION_METADATA = requisitionMetadata {
  externalKey = REQUISITION_KEY
  details = requisitionDetails { nonceHash = NONCE_HASH.bytes }
}
private val HMSS_REQUISITION_METADATA = requisitionMetadata {
  externalKey = REQUISITION_KEY
  details = requisitionDetails {
    externalFulfillingDuchyId = DUCHY_ID
    nonceHash = NONCE_HASH.bytes
  }
}
private val TRUS_TEE_REQUISITION_METADATA = requisitionMetadata {
  externalKey = REQUISITION_KEY
  details = requisitionDetails {
    externalFulfillingDuchyId = DUCHY_ID
    nonceHash = NONCE_HASH.bytes
  }
}
private val REQUISITION_BLOB_CONTEXT = RequisitionBlobContext(COMPUTATION_ID, REQUISITION_API_ID)

/** Test for [RequisitionFulfillmentService]. */
@RunWith(JUnit4::class)
class RequisitionFulfillmentServiceTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService()
  private val computationsServiceMock: ComputationsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(computationsServiceMock)
  }

  private lateinit var requisitionStore: RequisitionStore
  private lateinit var service: RequisitionFulfillmentService

  @Before
  fun initService() {
    requisitionStore = RequisitionStore(InMemoryStorageClient())
    service =
      RequisitionFulfillmentService(
        DUCHY_ID,
        RequisitionsCoroutineStub(grpcTestServerRule.channel),
        ComputationsCoroutineStub(grpcTestServerRule.channel),
        requisitionStore,
        Dispatchers.Default,
      )
  }

  @Test
  fun `fulfillRequisition writes new data to blob`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationStage = computationStage {
        liquidLegionsSketchAggregationV2 = Llv2Stage.INITIALIZATION_PHASE
      }
      computationDetails = COMPUTATION_DETAILS
      requisitions += REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val response =
      withPrincipal(DATA_PROVIDER_PRINCIPAL) {
        service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA))
      }

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    val blob = assertNotNull(requisitionStore.get(REQUISITION_BLOB_CONTEXT))
    assertThat(blob).contentEqualTo(TEST_REQUISITION_DATA)
    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionFulfillment,
      )
      .isEqualTo(
        recordRequisitionFulfillmentRequest {
          token = fakeToken
          key = REQUISITION_KEY
          blobPath = blob.blobKey
          publicApiVersion = Version.V2_ALPHA.string
        }
      )
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        systemFulfillRequisitionRequest {
          name = SYSTEM_REQUISITION_KEY.toName()
          nonce = NONCE
        }
      )
  }

  @Test
  fun `fulfillRequisition skips writing when requisition already fulfilled locally`() {
    val blobKey = REQUISITION_BLOB_CONTEXT.blobKey
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationDetails = COMPUTATION_DETAILS
      requisitions += REQUISITION_METADATA.copy { path = blobKey }
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }

    val response: FulfillRequisitionResponse =
      withPrincipal(DATA_PROVIDER_PRINCIPAL) {
        runBlocking { service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA)) }
      }

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    // The blob is not created since it is marked already fulfilled.
    assertThat(runBlocking { requisitionStore.get(blobKey) }).isNull()

    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        systemFulfillRequisitionRequest {
          name = SYSTEM_REQUISITION_KEY.toName()
          nonce = NONCE
        }
      )
  }

  @Test
  fun `fulfillRequisition writes the data and seed for HMSS protocol`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationStage = computationStage { honestMajorityShareShuffle = HmssStage.INITIALIZED }
      computationDetails = COMPUTATION_DETAILS
      requisitions += HMSS_REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val response =
      withPrincipal(DATA_PROVIDER_PRINCIPAL) {
        service.fulfillRequisition(HMSS_HEADER.withContent(TEST_REQUISITION_DATA))
      }

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    val blob = assertNotNull(requisitionStore.get(REQUISITION_BLOB_CONTEXT))
    assertThat(blob).contentEqualTo(TEST_REQUISITION_DATA)
    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionFulfillment,
      )
      .isEqualTo(
        recordRequisitionFulfillmentRequest {
          token = fakeToken
          key = REQUISITION_KEY
          blobPath = blob.blobKey
          publicApiVersion = Version.V2_ALPHA.string
          protocolDetails =
            RequisitionDetailsKt.requisitionProtocol {
              honestMajorityShareShuffle =
                RequisitionDetailsKt.RequisitionProtocolKt.honestMajorityShareShuffle {
                  secretSeedCiphertext = TEST_REQUISITION_SEED.ciphertext
                  registerCount = REGISTER_COUNT
                  dataProviderCertificate = DATA_PROVIDER_CERTIFICATE
                }
            }
        }
      )
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        systemFulfillRequisitionRequest {
          name = SYSTEM_REQUISITION_KEY.toName()
          nonce = NONCE
        }
      )
  }

  @Test
  fun `fulfillRequisiiton fails when seed is not specified for HMSS protocol`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationStage = computationStage { honestMajorityShareShuffle = HmssStage.INITIALIZED }
      computationDetails = COMPUTATION_DETAILS
      requisitions += HMSS_REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val request = HEADER.withContent(TEST_REQUISITION_DATA)
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        withPrincipal(DATA_PROVIDER_PRINCIPAL) { service.fulfillRequisition(request) }
      }

    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.message).contains("seed")
  }

  @Test
  fun `fulfillRequisition writes plain fulfillment data for TrusTEE protocol`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationStage = computationStage { trusTee = TrusTeeStage.INITIALIZED }
      computationDetails = COMPUTATION_DETAILS
      requisitions += TRUS_TEE_REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val response =
      withPrincipal(DATA_PROVIDER_PRINCIPAL) {
        service.fulfillRequisition(PLAIN_TRUS_TEE_HEADER.withContent(TEST_REQUISITION_DATA))
      }

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    val blob = assertNotNull(requisitionStore.get(REQUISITION_BLOB_CONTEXT))
    assertThat(blob).contentEqualTo(TEST_REQUISITION_DATA)
    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionFulfillment,
      )
      .isEqualTo(
        recordRequisitionFulfillmentRequest {
          token = fakeToken
          key = REQUISITION_KEY
          blobPath = blob.blobKey
          publicApiVersion = Version.V2_ALPHA.string
          protocolDetails =
            RequisitionDetailsKt.requisitionProtocol {
              trusTee =
                RequisitionDetailsKt.RequisitionProtocolKt.trusTee {
                  populationSpecFingerprint = POPULATION_SPEC_FINGERPRINT
                }
            }
        }
      )
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        systemFulfillRequisitionRequest {
          name = SYSTEM_REQUISITION_KEY.toName()
          nonce = NONCE
        }
      )
  }

  @Test
  fun `fulfillRequisition writes encrypted fulfillment data for TrusTEE protocol`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationStage = computationStage { trusTee = TrusTeeStage.INITIALIZED }
      computationDetails = COMPUTATION_DETAILS
      requisitions += TRUS_TEE_REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val response =
      withPrincipal(DATA_PROVIDER_PRINCIPAL) {
        service.fulfillRequisition(ENCRYPTED_TRUS_TEE_HEADER.withContent(TEST_REQUISITION_DATA))
      }

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    val blob = assertNotNull(requisitionStore.get(REQUISITION_BLOB_CONTEXT))
    assertThat(blob).contentEqualTo(TEST_REQUISITION_DATA)
    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionFulfillment,
      )
      .isEqualTo(
        recordRequisitionFulfillmentRequest {
          token = fakeToken
          key = REQUISITION_KEY
          blobPath = blob.blobKey
          publicApiVersion = Version.V2_ALPHA.string
          protocolDetails =
            RequisitionDetailsKt.requisitionProtocol {
              trusTee =
                RequisitionDetailsKt.RequisitionProtocolKt.trusTee {
                  encryptedDekCiphertext = ENCRYPTED_DEK_DATA
                  kmsKekUri = KMS_KEK_URI
                  workloadIdentityProvider = WORKLOAD_IDENTITY_PROVIDER
                  impersonatedServiceAccount = IMPERSONATED_SERVICE_ACCOUNT
                  populationSpecFingerprint = POPULATION_SPEC_FINGERPRINT
                }
            }
        }
      )
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        systemFulfillRequisitionRequest {
          name = SYSTEM_REQUISITION_KEY.toName()
          nonce = NONCE
        }
      )
  }

  @Test
  fun `fulfill requisition fails due to missing nonce`() = runBlocking {
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        withPrincipal(DATA_PROVIDER_PRINCIPAL) {
          service.fulfillRequisition(
            HEADER.toBuilder().clearNonce().build().withContent(TEST_REQUISITION_DATA)
          )
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e).hasMessageThat().contains("nonce")
  }

  @Test
  fun `fulfill requisition fails due to computation not found`() = runBlocking {
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        withPrincipal(DATA_PROVIDER_PRINCIPAL) {
          service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA))
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(e.message).contains("No computation is expecting this requisition")
  }

  @Test
  fun `fulfill requisition fails due nonce mismatch`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationDetails = COMPUTATION_DETAILS
      requisitions += REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }

    val e =
      assertFailsWith(StatusRuntimeException::class) {
        withPrincipal(DATA_PROVIDER_PRINCIPAL) {
          service.fulfillRequisition(
            HEADER.copy {
                nonce = 404L // Mismatching nonce value.
              }
              .withContent(TEST_REQUISITION_DATA)
          )
        }
      }

    assertThat(e.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(e).hasMessageThat().contains("verif")
  }

  @Test
  fun `request from unauthorized user should fail`() = runBlocking {
    val headerFromNonOwner = header {
      name = CanonicalRequisitionKey("Another EDP", REQUISITION_API_ID).toName()
      requisitionFingerprint = REQUISITION_FINGERPRINT
      nonce = NONCE
    }
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        withPrincipal(DATA_PROVIDER_PRINCIPAL) {
          service.fulfillRequisition(headerFromNonOwner.withContent(TEST_REQUISITION_DATA))
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(e).hasMessageThat().contains(headerFromNonOwner.name)
  }

  private fun FulfillRequisitionRequest.Header.withContent(
    vararg bodyContent: ByteString
  ): Flow<FulfillRequisitionRequest> {
    return bodyContent
      .asSequence()
      .map { fulfillRequisitionRequest { bodyChunk = bodyChunk { data = it } } }
      .asFlow()
      .onStart { emit(fulfillRequisitionRequest { header = this@withContent }) }
  }

  companion object {
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_API_ID)
    private val DATA_PROVIDER_PRINCIPAL = DataProviderPrincipal(DATA_PROVIDER_KEY)
  }
}
