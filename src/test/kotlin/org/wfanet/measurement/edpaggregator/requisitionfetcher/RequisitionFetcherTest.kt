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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.storage.requisitionBatch
import org.wfanet.measurement.api.v2alpha.copy


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

  @Test
  fun `fetchAndStoreRequisitions stores Requisition`() {
    val storage = LocalStorageHelper.getOptions().service
    val storageClient = GcsStorageClient(storage, BUCKET)
    val fetcher = RequisitionFetcher(requisitionsStub, storageClient, DATA_PROVIDER_NAME, 50, STORAGE_PATH_PREFIX)

    val expectedResult = requisitionBatch {
      requisitions += listOf(Any.pack(REQUISITION))
    }
    val persistedRequisition = runBlocking {
      fetcher.fetchAndStoreRequisitions()
      storageClient.getBlob("$STORAGE_PATH_PREFIX/${REQUISITION.name}")?.read()?.toByteArray()?.toByteString()
    }
    assertThat(expectedResult.toByteString()).isEqualTo(persistedRequisition)
  }

  @Test
  fun `fetchAndStoreRequisitions stores multiple Requisitions`() {
    val storage = LocalStorageHelper.getOptions().service
    val storageClient = GcsStorageClient(storage, BUCKET)
    val fetcher = RequisitionFetcher(requisitionsStub, storageClient, DATA_PROVIDER_NAME, 50, STORAGE_PATH_PREFIX)

    val requisitionsList = List(100) {
      REQUISITION.copy {
        name = REQUISITION_NAME + it.toString()
      }
    }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisitionsList })
    }

    val expectedResult = requisitionsList.map {
      requisitionBatch {
        requisitions += listOf(Any.pack(it))
      }
    }
    runBlocking {
      fetcher.fetchAndStoreRequisitions()
      expectedResult.map {
        val requisition = it.requisitionsList[0].unpack(Requisition::class.java)
        assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/${requisition.name}")).isNotNull()
      }
    }
  }

  companion object {
    private const val STORAGE_PATH_PREFIX = "test-requisitions/"
    private const val BUCKET = "requisition-storage-test-bucket"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "${DATA_PROVIDER_NAME}/requisitions/foo"
    private val REQUISITION = requisition {
      name = REQUISITION_NAME
    }
  }
}
