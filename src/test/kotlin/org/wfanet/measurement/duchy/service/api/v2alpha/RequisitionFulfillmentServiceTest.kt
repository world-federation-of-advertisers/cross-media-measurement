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
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
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
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.storage.StorageClient
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
private val TEST_REQUISITION_DATA = ByteString.copyFromUtf8("some data")
private val SIGNATURE = ByteString.copyFromUtf8("a signature")
private val HEADER =
  FulfillRequisitionRequest.Header.newBuilder()
    .apply {
      name = RequisitionKey(DATA_PROVIDER_ID, REQUISITION_ID).toName()
      dataProviderParticipationSignature = SIGNATURE
    }
    .build()
private val FULFILLED_RESPONSE =
  FulfillRequisitionResponse.newBuilder().apply { state = Requisition.State.FULFILLED }.build()
private val REQUISITION_KEY = SystemRequisitionKey(COMPUTATION_ID, REQUISITION_ID)

/** Test for [RequisitionFulfillmentService]. */
@RunWith(JUnit4::class)
class RequisitionFulfillmentServiceTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val computationsServiceMock: ComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  private val tempDirectory = TemporaryFolder()
  private lateinit var requisitionStore: RequisitionStore
  private suspend fun StorageClient.readBlobAsString(key: String): String {
    return getBlob(key)?.read(defaultBufferSizeBytes)?.flatten()?.toStringUtf8()!!
  }

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
    val fakeToken =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = COMPUTATION_ID
          addRequisitionsBuilder().apply {
            externalDataProviderId = DATA_PROVIDER_ID
            externalRequisitionId = REQUISITION_ID
          }
        }
        .build()

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
            keyBuilder.apply {
              externalDataProviderId = DATA_PROVIDER_ID
              externalRequisitionId = REQUISITION_ID
            }
            blobPath = NEXT_BLOB_PATH
          }
          .build()
      )

    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillRequisition)
      .isEqualTo(
        SystemFulfillRequisitionRequest.newBuilder()
          .apply {
            name = REQUISITION_KEY.toName()
            dataProviderParticipationSignature = SIGNATURE
          }
          .build()
      )
  }

  @Test
  fun `fulfill requisition, already fulfilled locally should skip writing`() = runBlocking {
    val fakeToken =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = COMPUTATION_ID
          addRequisitionsBuilder().apply {
            externalDataProviderId = DATA_PROVIDER_ID
            externalRequisitionId = REQUISITION_ID
            path = NEXT_BLOB_PATH
          }
        }
        .build()
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
            name = REQUISITION_KEY.toName()
            dataProviderParticipationSignature = SIGNATURE
          }
          .build()
      )
  }

  @Test
  fun `fulfill requisition fails due to missing signature`() = runBlocking {
    val e =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          HEADER
            .toBuilder()
            .clearDataProviderParticipationSignature()
            .build()
            .withContent(TEST_REQUISITION_DATA)
        )
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.message).contains("DataProviderParticipationSignature is missing in the header.")
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
