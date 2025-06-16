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
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry

class TestIdGenerator() : IdGenerator {
  var next = AtomicLong(1)

  override fun generateId(): Long {
    return next.getAndIncrement()
  }

  fun reset(): Unit {
    next = AtomicLong(1)
  }

}

@RunWith(JUnit4::class)
class RequisitionFetcherTest {
  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { })
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionGrouper: RequisitionGrouper = mock()

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `fetchAndStoreRequisitions stores single GroupedRequisition`() = runBlocking {

    whenever(requisitionGrouper.groupRequisitions(any()))
      .thenReturn(listOf(GROUPED_REQUISITIONS))

    val storageClient = FileSystemStorageClient(tempFolder.root)
    val idGenerator = TestIdGenerator()
    val groupedRequisitionsId = idGenerator.next
    val blobKey = "$STORAGE_PATH_PREFIX/${groupedRequisitionsId}"
    val fetcher =
      RequisitionFetcher(requisitionsStub, storageClient, DATA_PROVIDER_NAME, STORAGE_PATH_PREFIX, requisitionGrouper, idGenerator)
    val typeRegistry = TypeRegistry.newBuilder().add(Requisition.getDescriptor()).build()

      fetcher.fetchAndStoreRequisitions()
      val blob = storageClient.getBlob(blobKey)
      assertThat(blob).isNotNull()
      val blobContent: ByteString = blob!!.read().flatten()
    val parsedBlob = Any.parseFrom(blobContent)
    assertThat(parsedBlob)
      .unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry())
      .isEqualTo(Any.pack(GROUPED_REQUISITIONS))
  }

  @Test
  fun `fetchAndStoreRequisitions stores multiple GroupedRequisitions`() {

    runBlocking {
      val groupedRequisitionsList: List<GroupedRequisitions> =
        listOf(GROUPED_REQUISITIONS, GROUPED_REQUISITIONS, GROUPED_REQUISITIONS)
      whenever(requisitionGrouper.groupRequisitions(any()))
        .thenReturn(groupedRequisitionsList)

      val storageClient = FileSystemStorageClient(tempFolder.root)
      val idGenerator = TestIdGenerator()
      val fetcher =
        RequisitionFetcher(
          requisitionsStub,
          storageClient,
          DATA_PROVIDER_NAME,
          STORAGE_PATH_PREFIX,
          requisitionGrouper,
          idGenerator
        )

      val expectedResult = groupedRequisitionsList.map { Any.pack(it) }
      fetcher.fetchAndStoreRequisitions()
      idGenerator.reset()
      expectedResult.map {
        assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/${idGenerator.next}")).isNotNull()
      }
    }
  }

  companion object {
    private const val STORAGE_PATH_PREFIX = "test-requisitions"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private val REQUISITION = requisition {
      name = "requisition-name"
    }
    private val GROUPED_REQUISITIONS =
      groupedRequisitions {
        requisitions.add(
          requisitionEntry {
            requisition = Any.pack(
              REQUISITION
            )
          }
        )
      }
  }
}


//private val REQUISITION: Requisition = requisition {
//      name = REQUISITION_NAME
//      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
//      state = Requisition.State.UNFULFILLED
//      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
//      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
//      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
//      protocolConfig = protocolConfig {
//        protocols +=
//          ProtocolConfigKt.protocol {
//            direct =
//              ProtocolConfigKt.direct {
//                noiseMechanisms +=
//                  listOf(
//                    ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
//                    ProtocolConfig.NoiseMechanism.NONE,
//                  )
//                deterministicCountDistinct =
//                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
//                deterministicDistribution =
//                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
//              }
//          }
//      }
//      dataProviderCertificate = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
//      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
//    }
