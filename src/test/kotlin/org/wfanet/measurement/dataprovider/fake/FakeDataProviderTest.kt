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
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.CombinedPublicKey
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.SketchConfig
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.throttler.testing.FakeThrottler
import org.wfanet.measurement.dataprovider.common.ElGamalPublicKeyStore
import org.wfanet.measurement.dataprovider.common.EncryptedSketchGenerator
import org.wfanet.measurement.dataprovider.common.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.common.SketchConfigStore
import org.wfanet.measurement.dataprovider.common.UnfulfilledRequisitionProvider

private val DATA_PROVIDER_ID = ExternalId(123)

private val COMBINED_PUBLIC_KEY_KEY: CombinedPublicKey.Key =
  CombinedPublicKey.Key.newBuilder().apply {
    combinedPublicKeyId = "some-combined-public-key-id"
  }.build()

private val ENCRYPTION_KEY: ElGamalPublicKey =
  ElGamalPublicKey.newBuilder().apply {
    ellipticCurveId = 123456789
    generator = byteStringOf(0xAB, 0xCD)
    element = byteStringOf(0xEF, 0x89)
  }.build()

private val SKETCH_CONFIG = SketchConfig.newBuilder().apply {
  keyBuilder.sketchConfigId = "some-sketch-config-id"
}.build()

private val REQUISITION = Requisition.newBuilder().apply {
  measurementSpecBuilder.data = MeasurementSpec.newBuilder().apply {
    encryptedSketchBuilder.apply {
      combinedPublicKey = COMBINED_PUBLIC_KEY_KEY
      sketchConfig = SKETCH_CONFIG.key
    }
  }.build().toByteString()
}.build()

@RunWith(JUnit4::class)
class FakeDataProviderTest {

  @Test
  fun basicOperation() = runBlocking<Unit> {
    val throttler = FakeThrottler()

    val mockSketchGenerator = mock<EncryptedSketchGenerator>()
    val mockUnfulfilledRequisitionProvider = mock<UnfulfilledRequisitionProvider>()
    val mockRequisitionFulfiller = mock<RequisitionFulfiller>()
    val mockElGamalPublicKeyStore = mock<ElGamalPublicKeyStore>()
    val mockSketchConfigStore = mock<SketchConfigStore>()

    val fakeDataProvider =
      FakeDataProvider(
        mockSketchGenerator,
        mockUnfulfilledRequisitionProvider,
        mockRequisitionFulfiller,
        mockElGamalPublicKeyStore,
        mockSketchConfigStore,
        throttler,
        streamByteBufferSize = 1
      )

    val latch = CountDownLatch(2)

    whenever(mockUnfulfilledRequisitionProvider.get())
      .thenAnswer {
        latch.countDown()
        null
      }
      .thenAnswer {
        latch.countDown()
        REQUISITION
      }
      .thenAnswer {
        latch.countDown()
        null
      }

    whenever(mockElGamalPublicKeyStore.get(any()))
      .thenReturn(ENCRYPTION_KEY)

    val observedRequisitionData = mutableListOf<List<ByteString>>()
    whenever(mockRequisitionFulfiller.fulfillRequisition(any(), any()))
      .thenAnswer {
        val byteStrings = runBlocking { it.getArgument<Flow<ByteString>>(1).toList() }
        observedRequisitionData.add(byteStrings)
      }

    whenever(mockSketchConfigStore.get(any()))
      .thenReturn(SKETCH_CONFIG)

    whenever(mockSketchGenerator.generate(any(), any(), any()))
      .thenReturn(byteStringOf(0x01, 0x02))

    val fakeDataProviderCoroutine = launch { fakeDataProvider.start() }

    latch.await()
    fakeDataProviderCoroutine.cancelAndJoin()

    assertThat(observedRequisitionData)
      .containsExactly(listOf(byteStringOf(0x01), byteStringOf(0x02)))

    verifyProtoArgument(mockElGamalPublicKeyStore, ElGamalPublicKeyStore::get)
      .isEqualTo(COMBINED_PUBLIC_KEY_KEY)

    verifyProtoArgument(mockSketchConfigStore, SketchConfigStore::get)
      .isEqualTo(SKETCH_CONFIG.key)
  }
}
