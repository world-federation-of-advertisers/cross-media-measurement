// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider.fake

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineImplBase
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.api.v1alpha.UploadMetricValueResponse
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.throttler.testing.FakeThrottler

private val DATA_PROVIDER_ID = ExternalId(123)

private val ENCRYPTION_KEY: ElGamalPublicKey =
  ElGamalPublicKey.newBuilder().apply {
    ellipticCurveId = 123456789
    generator = byteStringOf(0xAB, 0xCD)
    element = byteStringOf(0xEF, 0x89)
  }.build()

@RunWith(JUnit4::class)
class FakeDataProviderTest {

  private val publisherDataService: PublisherDataCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(publisherDataService) }

  private val stub by lazy { PublisherDataCoroutineStub(grpcTestServerRule.channel) }

  @Test
  fun basicOperation() = runBlocking<Unit> {
    val throttler = FakeThrottler()

    val latch = CountDownLatch(3)
    val observedUploads = mutableListOf<List<UploadMetricValueRequest>>()

    val metricRequisition1 = makeMetricRequisition(1)
    val metricRequisition2 = makeMetricRequisition(2)

    publisherDataService.stub {
      onBlocking { uploadMetricValue(any()) }.thenAnswer {
        val requests: Flow<UploadMetricValueRequest> = it.getArgument(0)
        runBlocking { observedUploads.add(requests.toList()) }
        UploadMetricValueResponse.getDefaultInstance()
      }

      onBlocking { listMetricRequisitions(any()) }
        .thenAnswer {
          latch.countDown()
          makeListMetricRequisitionsResponse(metricRequisition1, "page-token-1")
        }
        .thenAnswer {
          latch.countDown()
          makeListMetricRequisitionsResponse(metricRequisition2, "page-token-2")
        }
        .thenAnswer {
          latch.countDown()
          ListMetricRequisitionsResponse.getDefaultInstance()
        }

      onBlocking { getCombinedPublicKey(any()) }
        .thenAnswer {
          CombinedPublicKey.newBuilder().apply {
            key = it.getArgument<GetCombinedPublicKeyRequest>(0).key
            encryptionKey = ENCRYPTION_KEY
          }.build()
        }
    }

    val observedMetricRequisitions = mutableListOf<MetricRequisition>()
    val observedPublicKeys = mutableListOf<ElGamalPublicKey>()
    val fakeSketchGenerator = { metricRequisition: MetricRequisition, key: ElGamalPublicKey ->
      observedMetricRequisitions.add(metricRequisition)
      observedPublicKeys.add(key)
      byteStringOf(0x01, 0x02)
    }

    val fakeDataProvider =
      FakeDataProvider(
        publisherDataStub = stub,
        externalDataProviderId = DATA_PROVIDER_ID,
        throttler = throttler,
        generateSketch = fakeSketchGenerator,
        streamByteBufferSize = 1
      )

    val fakeDataProviderCoroutine = launch { fakeDataProvider.start() }

    latch.await()
    fakeDataProviderCoroutine.cancelAndJoin()

    assertThat(observedUploads)
      .containsExactly(
        listOf(
          makeExpectedHeader(metricRequisition1.key),
          makeExpectedBody(0x01),
          makeExpectedBody(0x02)
        ),
        listOf(
          makeExpectedHeader(metricRequisition2.key),
          makeExpectedBody(0x01),
          makeExpectedBody(0x02)
        )
      )
      .inOrder()

    assertThat(observedMetricRequisitions)
      .containsExactly(metricRequisition1, metricRequisition2)

    assertThat(observedPublicKeys)
      .containsExactly(ENCRYPTION_KEY, ENCRYPTION_KEY)

    argumentCaptor<ListMetricRequisitionsRequest> {
      verify(publisherDataService, times(3)).listMetricRequisitions(capture())
      assertThat(allValues)
        .comparingExpectedFieldsOnly()
        .containsExactly(
          makeExpectedListMetricRequisitionsRequest(""),
          makeExpectedListMetricRequisitionsRequest("page-token-1"),
          makeExpectedListMetricRequisitionsRequest("page-token-2")
        )
        .inOrder()
    }
  }

  private fun makeExpectedHeader(key: MetricRequisition.Key): UploadMetricValueRequest {
    return UploadMetricValueRequest.newBuilder().apply {
      headerBuilder.key = key
    }.build()
  }

  private fun makeExpectedBody(data: Byte): UploadMetricValueRequest {
    return UploadMetricValueRequest.newBuilder().apply {
      chunkBuilder.data = byteStringOf(data.toInt())
    }.build()
  }

  private fun makeExpectedListMetricRequisitionsRequest(
    pageToken: String
  ): ListMetricRequisitionsRequest {
    return ListMetricRequisitionsRequest.newBuilder().also {
      it.parentBuilder.dataProviderId = DATA_PROVIDER_ID.apiId.value
      it.pageToken = pageToken
    }.build()
  }

  private fun makeMetricRequisition(index: Int): MetricRequisition {
    return MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = "abc"
        campaignId = "def"
        metricRequisitionId = "metric-requisition-$index"
      }
      combinedPublicKeyBuilder.combinedPublicKeyId = "combined-public-key-$index"
    }.build()
  }

  private fun makeListMetricRequisitionsResponse(
    metricRequisition: MetricRequisition,
    nextPageToken: String
  ): ListMetricRequisitionsResponse {
    return ListMetricRequisitionsResponse.newBuilder().also {
      it.addMetricRequisitions(metricRequisition)
      it.nextPageToken = nextPageToken
    }.build()
  }
}
