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

package org.wfanet.measurement.duchy.service.internal.metricvalues

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verifyBlocking
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.security.MessageDigest
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.size
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.duchy.storage.MetricValueStore
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.StoreMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueResponse
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

@RunWith(JUnit4::class)
class MetricValuesServiceTest {
  @Rule
  @JvmField
  val tempDirectory = TemporaryFolder()

  private var nextBlobKey = ""
  private val metricValueDbMock: MetricValueDatabase = mock()
  private lateinit var metricValueStore: MetricValueStore
  private lateinit var service: MetricValuesService

  @Before fun initService() {
    val storageClient = FileSystemStorageClient(tempDirectory.root)
    metricValueStore = MetricValueStore.forTesting(storageClient) { nextBlobKey }
    service = MetricValuesService.forTesting(metricValueDbMock, metricValueStore)
  }

  @Test fun `getMetricValue by ID returns MetricValue`() = runBlocking {
    metricValueDbMock.stub {
      onBlocking {
        getMetricValue(ExternalId(testMetricValue.externalId))
      }.thenReturn(testMetricValue)
    }

    val response = service.getMetricValue(
      GetMetricValueRequest.newBuilder().apply {
        externalId = testMetricValue.externalId
      }.build()
    )

    assertThat(response).isEqualTo(testMetricValue)
  }

  @Test fun `getMetricValue by resource key returns MetricValue`() = runBlocking {
    metricValueDbMock.stub {
      onBlocking {
        getMetricValue(testMetricValue.resourceKey)
      }.thenReturn(testMetricValue)
    }

    val response = service.getMetricValue(
      GetMetricValueRequest.newBuilder().apply {
        resourceKey = testMetricValue.resourceKey
      }.build()
    )

    assertThat(response).isEqualTo(testMetricValue)
  }

  @Test fun `getMetricValue throws INVALID_ARGUMENT when key not set`() = runBlocking {
    val e = assertFailsWith(StatusRuntimeException::class) {
      service.getMetricValue(GetMetricValueRequest.getDefaultInstance())
    }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test fun `getMetricValue throws NOT_FOUND when MetricValue not found`() = runBlocking {
    val e = assertFailsWith(StatusRuntimeException::class) {
      service.getMetricValue(
        GetMetricValueRequest.newBuilder().apply {
          resourceKey = testMetricValue.resourceKey
        }.build()
      )
    }
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `storeMetricValue stores MetricValue with data`() = runBlocking {
    nextBlobKey = testMetricValue.blobStorageKey

    metricValueDbMock.stub {
      onBlocking {
        insertMetricValue(any())
      }.thenReturn(testMetricValue)
    }

    val response =
      service.storeMetricValue(
        flowOf(
          StoreMetricValueRequest.newBuilder()
            .apply { headerBuilder.resourceKey = testMetricValue.resourceKey }
            .build(),
          StoreMetricValueRequest.newBuilder()
            .apply { chunkBuilder.data = testMetricValueData }
            .build()
        )
      )

    argumentCaptor<MetricValue> {
      verifyBlocking(metricValueDbMock, times(1)) {
        insertMetricValue(capture())
      }
      assertThat(firstValue).isEqualTo(testMetricValue.toBuilder().clearExternalId().build())
    }
    assertThat(response).isEqualTo(testMetricValue)
    val data = assertNotNull(metricValueStore.get(testMetricValue.blobStorageKey))
    assertThat(data).contentEqualTo(testMetricValueData)
  }

  @Test fun `streamMetricValue returns MetricValue with data`() = runBlocking {
    nextBlobKey = testMetricValue.blobStorageKey
    metricValueStore.write(flowOf(testMetricValueData))

    metricValueDbMock.stub {
      onBlocking {
        getMetricValue(testMetricValue.resourceKey)
      }.thenReturn(testMetricValue)
    }

    lateinit var header: StreamMetricValueResponse.Header
    var output = ByteString.EMPTY
    service.streamMetricValue(
      StreamMetricValueRequest.newBuilder().apply {
        resourceKey = testMetricValue.resourceKey
      }.build()
    ).collect { responseMessage ->
      if (responseMessage.hasHeader()) {
        header = responseMessage.header
      } else {
        output = output.concat(responseMessage.chunk.data)
      }
    }

    assertThat(header.metricValue).isEqualTo(testMetricValue)
    assertThat(header.dataSizeBytes).isEqualTo(testMetricValueData.size)
    assertThat(output).isEqualTo(testMetricValueData)
  }

  @Test fun `streamMetricValue throws when fingerprint is wrong`() = runBlocking {
    nextBlobKey = testMetricValue.blobStorageKey
    metricValueStore.write(flowOf(testMetricValueData))

    metricValueDbMock.stub {
      onBlocking {
        getMetricValue(testMetricValue.resourceKey)
      }.thenReturn(
        testMetricValue.toBuilder()
          .setBlobFingerprint(random.nextBytes(32).toByteString())
          .build()
      )
    }

    val e = assertFailsWith(StatusRuntimeException::class) {
      service.streamMetricValue(
        StreamMetricValueRequest.newBuilder().apply {
          resourceKey = testMetricValue.resourceKey
        }.build()
      ).collect()
    }

    assertThat(e.status.code).isEqualTo(Status.Code.DATA_LOSS)
  }

  @Test fun `streamMetricValue throws INVALID_ARGUMENT when key not set`() = runBlocking {
    val e = assertFailsWith(StatusRuntimeException::class) {
      service.streamMetricValue(StreamMetricValueRequest.getDefaultInstance()).collect()
    }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test fun `streamMetricValue throws NOT_FOUND when MetricValue not found`() = runBlocking {
    val e = assertFailsWith(StatusRuntimeException::class) {
      service.streamMetricValue(
        StreamMetricValueRequest.newBuilder().apply {
          resourceKey = testMetricValue.resourceKey
        }.build()
      ).collect()
    }
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test fun `streamMetricValue throws DATA_LOSS when blob not found`() = runBlocking {
    nextBlobKey = testMetricValue.blobStorageKey

    metricValueDbMock.stub {
      onBlocking {
        getMetricValue(testMetricValue.resourceKey)
      }.thenReturn(testMetricValue)
    }

    val e = assertFailsWith(StatusRuntimeException::class) {
      service.streamMetricValue(
        StreamMetricValueRequest.newBuilder().apply {
          resourceKey = testMetricValue.resourceKey
        }.build()
      ).collect()
    }
    assertThat(e.status.code).isEqualTo(Status.Code.DATA_LOSS)
  }

  companion object {
    private val random = Random.Default
    private val testMetricValueData: ByteString =
      random.nextBytes(random.nextInt(BYTES_PER_MIB * 3, BYTES_PER_MIB * 4)).toByteString()

    private val testMetricValueDataFingerprint: ByteString =
      MessageDigest
        .getInstance("SHA-256")
        .digest(testMetricValueData.toByteArray())
        .toByteString()

    private val testMetricValue: MetricValue = MetricValue.newBuilder().apply {
      externalId = 987654321L
      resourceKeyBuilder.apply {
        dataProviderResourceId = "data-provider-id"
        campaignResourceId = "campaign-id"
        metricRequisitionResourceId = "requisition-id"
      }
      blobStorageKey = "blob-key"
      blobFingerprint = testMetricValueDataFingerprint
    }.build()
  }
}
