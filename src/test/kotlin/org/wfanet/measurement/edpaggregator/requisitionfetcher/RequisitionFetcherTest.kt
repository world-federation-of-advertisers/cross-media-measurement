/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {
  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `fetchAndStoreRequisitions stores Requisition`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val fetcher =
      RequisitionFetcher(requisitionsStub, storageClient, DATA_PROVIDER_NAME, STORAGE_PATH_PREFIX)
    val typeRegistry = TypeRegistry.newBuilder().add(Requisition.getDescriptor()).build()
    val parsedBlob = runBlocking {
      fetcher.fetchAndStoreRequisitions()
      val blob = storageClient.getBlob("$STORAGE_PATH_PREFIX/${REQUISITION.name}")
      assertThat(blob).isNotNull()
      val blobContent: ByteString = blob!!.read().flatten()
      Any.parseFrom(blobContent)
    }
    assertThat(parsedBlob)
      .unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry())
      .isEqualTo(Any.pack(REQUISITION))
  }

  @Test
  fun `fetchAndStoreRequisitions stores multiple Requisitions`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val fetcher =
      RequisitionFetcher(
        requisitionsStub,
        storageClient,
        DATA_PROVIDER_NAME,
        STORAGE_PATH_PREFIX,
        50,
      )

    val requisitionsList =
      List(100) { REQUISITION.copy { name = REQUISITION_NAME + it.toString() } }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisitionsList })
    }

    val expectedResult = requisitionsList.map { Any.pack(it) }
    runBlocking {
      fetcher.fetchAndStoreRequisitions()
      expectedResult.map {
        val requisition = it.unpack(Requisition::class.java)
        assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/${requisition.name}")).isNotNull()
      }
    }
  }

  companion object {
    private const val STORAGE_PATH_PREFIX = "test-requisitions/"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "${DATA_PROVIDER_NAME}/requisitions/foo"
    private val REQUISITION = requisition { name = REQUISITION_NAME }
  }
}
