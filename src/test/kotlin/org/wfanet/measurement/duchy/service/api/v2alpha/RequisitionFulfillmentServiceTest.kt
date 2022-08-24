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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
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
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.getComputationTokenResponse
import org.wfanet.measurement.internal.duchy.recordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.system.v1alpha.RequisitionKey as SystemRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.fulfillRequisitionRequest as systemFulfillRequisitionRequest

private const val COMPUTATION_ID = "xyz"
private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private val DATA_PROVIDER_API_ID = externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID)
private const val REQUISITION_API_ID = "abcd"
private const val NONCE = -3060866405677570814L // Hex: D5859E38A0A96502
private val NONCE_HASH =
  HexString("45FEAA185D434E0EB4747F547F0918AA5B8403DBBD7F90D6F0D8C536E2D620D7")
private val REQUISITION_FINGERPRINT = "A fingerprint".toByteStringUtf8()
private val TEST_REQUISITION_DATA = ByteString.copyFromUtf8("some data")
private val HEADER = header {
  name = RequisitionKey(DATA_PROVIDER_API_ID, REQUISITION_API_ID).toName()
  requisitionFingerprint = REQUISITION_FINGERPRINT
  nonce = NONCE
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
private val REQUISITION_BLOB_CONTEXT = RequisitionBlobContext(COMPUTATION_ID, REQUISITION_API_ID)

/** Test for [RequisitionFulfillmentService]. */
@RunWith(JUnit4::class)
class RequisitionFulfillmentServiceTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService()
  private val computationsServiceMock: ComputationsCoroutineImplBase = mockService()

  private val callerIdentityProvider = {
    provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = EXTERNAL_DATA_PROVIDER_ID
    }
  }

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
        RequisitionsCoroutineStub(grpcTestServerRule.channel),
        ComputationsCoroutineStub(grpcTestServerRule.channel),
        requisitionStore,
        callerIdentityProvider
      )
  }

  @Test
  fun `fulfill requisition writes new data to blob`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationDetails = COMPUTATION_DETAILS
      requisitions += REQUISITION_METADATA
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(getComputationTokenResponse { token = fakeToken })
    }
    RequisitionBlobContext(COMPUTATION_ID, HEADER.name)

    val response = service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA))

    assertThat(response).isEqualTo(FULFILLED_RESPONSE)
    val blob = assertNotNull(requisitionStore.get(REQUISITION_BLOB_CONTEXT))
    assertThat(blob).contentEqualTo(TEST_REQUISITION_DATA)
    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionBlobPath
      )
      .isEqualTo(
        recordRequisitionBlobPathRequest {
          token = fakeToken
          key = REQUISITION_KEY
          blobPath = blob.blobKey
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
  fun `fulfill requisition, already fulfilled locally should skip writing`() = runBlocking {
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

    assertThat(service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA)))
      .isEqualTo(FULFILLED_RESPONSE)
    // The blob is not created since it is marked already fulfilled.
    assertNull(requisitionStore.get(blobKey))

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
        service.fulfillRequisition(
          HEADER.toBuilder().clearNonce().build().withContent(TEST_REQUISITION_DATA)
        )
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
        service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA))
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
        service.fulfillRequisition(
          HEADER.copy {
              nonce = 404L // Mismatching nonce value.
            }
            .withContent(TEST_REQUISITION_DATA)
        )
      }

    assertThat(e.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(e).hasMessageThat().contains("verif")
  }

  @Test
  fun `request from unauthorized user should fail`() = runBlocking {
    val headerFromNonOwner = header {
      name = RequisitionKey("Another EDP", REQUISITION_API_ID).toName()
      requisitionFingerprint = REQUISITION_FINGERPRINT
      nonce = NONCE
    }
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(headerFromNonOwner.withContent(TEST_REQUISITION_DATA))
      }
    assertThat(e.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(e).hasMessageThat().contains("identity")
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
}
