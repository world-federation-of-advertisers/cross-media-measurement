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
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionDetails
import org.wfanet.measurement.internal.duchy.requisitionMetadata
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat
import org.wfanet.measurement.system.v1alpha.FulfillRequisitionRequest as SystemFulfillRequisitionRequest
import org.wfanet.measurement.system.v1alpha.RequisitionKey as SystemRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub

private const val COMPUTATION_ID = "xyz"
private const val DATA_PROVIDER_ID = "1234"
private const val REQUISITION_ID = "abcd"
private const val NEXT_BLOB_PATH = "just a path"
private const val NONCE = -3060866405677570814L // Hex: D5859E38A0A96502
private val NONCE_HASH =
  HexString("45FEAA185D434E0EB4747F547F0918AA5B8403DBBD7F90D6F0D8C536E2D620D7")
private val REQUISITION_FINGERPRINT = "A fingerprint".toByteStringUtf8()
private val TEST_REQUISITION_DATA = ByteString.copyFromUtf8("some data")
private val HEADER =
  FulfillRequisitionRequest.Header.newBuilder()
    .apply {
      name = RequisitionKey(DATA_PROVIDER_ID, REQUISITION_ID).toName()
      requisitionFingerprint = REQUISITION_FINGERPRINT
      nonce = NONCE
    }
    .build()
private val FULFILLED_RESPONSE =
  FulfillRequisitionResponse.newBuilder().apply { state = Requisition.State.FULFILLED }.build()
private val SYSTEM_REQUISITION_KEY = SystemRequisitionKey(COMPUTATION_ID, REQUISITION_ID)
private val REQUISITION_KEY = externalRequisitionKey {
  externalRequisitionId = REQUISITION_ID
  requisitionFingerprint = REQUISITION_FINGERPRINT
}
private val MEASUREMENT_SPEC = measurementSpec { nonceHashes += NONCE_HASH.bytes }
private val COMPUTATION_DETAILS = computationDetails {
  kingdomComputation =
    kingdomComputationDetails {
      publicApiVersion = Version.V2_ALPHA.string
      measurementSpec = MEASUREMENT_SPEC.toByteString()
    }
}
private val REQUISITION_METADATA = requisitionMetadata {
  externalKey = REQUISITION_KEY
  details = requisitionDetails { nonceHash = NONCE_HASH.bytes }
}

/** Test for [RequisitionFulfillmentService]. */
@RunWith(JUnit4::class)
class RequisitionFulfillmentServiceTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val computationsServiceMock: ComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  private val tempDirectory = TemporaryFolder()
  private lateinit var requisitionStore: RequisitionStore

  val grpcTestServerRule = GrpcTestServerRule {
    val storageClient = FileSystemStorageClient(tempDirectory.root)
    requisitionStore = RequisitionStore.forTesting(storageClient) { NEXT_BLOB_PATH }
    addService(requisitionsServiceMock)
    addService(computationsServiceMock)
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private lateinit var service: RequisitionFulfillmentService

  @Before
  fun initService() {
    service =
      RequisitionFulfillmentService(
        RequisitionsCoroutineStub(grpcTestServerRule.channel),
        ComputationsCoroutineStub(grpcTestServerRule.channel),
        requisitionStore
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
        .thenReturn(GetComputationTokenResponse.newBuilder().apply { token = fakeToken }.build())
    }

    assertThat(service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA)))
      .isEqualTo(FULFILLED_RESPONSE)
    val data = assertNotNull(requisitionStore.get(NEXT_BLOB_PATH))
    assertThat(data).contentEqualTo(TEST_REQUISITION_DATA)

    verifyProtoArgument(
        computationsServiceMock,
        ComputationsCoroutineImplBase::recordRequisitionBlobPath
      )
      .isEqualTo(
        RecordRequisitionBlobPathRequest.newBuilder()
          .apply {
            token = fakeToken
            key = REQUISITION_KEY
            blobPath = NEXT_BLOB_PATH
          }
          .build()
      )

    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        SystemFulfillRequisitionRequest.newBuilder()
          .apply {
            name = SYSTEM_REQUISITION_KEY.toName()
            nonce = NONCE
          }
          .build()
      )
  }

  @Test
  fun `fulfill requisition, already fulfilled locally should skip writing`() = runBlocking {
    val fakeToken = computationToken {
      globalComputationId = COMPUTATION_ID
      computationDetails = COMPUTATION_DETAILS
      requisitions += REQUISITION_METADATA.copy { path = NEXT_BLOB_PATH }
    }
    computationsServiceMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenReturn(GetComputationTokenResponse.newBuilder().apply { token = fakeToken }.build())
    }

    assertThat(service.fulfillRequisition(HEADER.withContent(TEST_REQUISITION_DATA)))
      .isEqualTo(FULFILLED_RESPONSE)
    // The blob is not created since it is marked already fulfilled.
    assertNull(requisitionStore.get(NEXT_BLOB_PATH))

    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        SystemFulfillRequisitionRequest.newBuilder()
          .apply {
            name = SYSTEM_REQUISITION_KEY.toName()
            nonce = NONCE
          }
          .build()
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
        .thenReturn(GetComputationTokenResponse.newBuilder().apply { token = fakeToken }.build())
    }

    val e =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          HEADER
            .copy {
              nonce = 404L // Mismatching nonce value.
            }
            .withContent(TEST_REQUISITION_DATA)
        )
      }

    assertThat(e.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(e).hasMessageThat().contains("verif")
  }

  private fun FulfillRequisitionRequest.Header.withContent(
    vararg bodyContent: ByteString
  ): Flow<FulfillRequisitionRequest> {
    return bodyContent
      .asSequence()
      .map {
        FulfillRequisitionRequest.newBuilder()
          .apply { bodyChunkBuilder.apply { data = it } }
          .build()
      }
      .asFlow()
      .onStart { emit(FulfillRequisitionRequest.newBuilder().setHeader(this@withContent).build()) }
  }
}
