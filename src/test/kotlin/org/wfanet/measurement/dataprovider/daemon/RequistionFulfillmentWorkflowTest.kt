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

package org.wfanet.measurement.dataprovider.daemon

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verifyBlocking
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.CombinedPublicKey
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.EncryptedSketch
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.SketchConfig
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.dataprovider.common.EncryptedSketchGenerator
import org.wfanet.measurement.dataprovider.common.RequisitionDecoder
import org.wfanet.measurement.dataprovider.common.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.common.UnfulfilledRequisitionProvider

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

private val REQUISITION_SPEC = RequisitionSpec.newBuilder().apply {
  // Add some nonsense to differentiate from the default instance.
  addEventGroupEntriesBuilder().eventGroupBuilder.eventGroupId = "some-event-group-id"
}.build()

private val ENCRYPTED_SKETCH = EncryptedSketch.newBuilder().apply {
  sketchConfig = SKETCH_CONFIG.key
  combinedPublicKey = COMBINED_PUBLIC_KEY_KEY
}.build()

private val MEASUREMENT_SPEC = MeasurementSpec.newBuilder().apply {
  encryptedSketch = ENCRYPTED_SKETCH
}.build()

private val REQUISITION = Requisition.newBuilder().apply {
  measurementSpecBuilder.data = MEASUREMENT_SPEC.toByteString()
  encryptedRequisitionSpec =
    SignedData.newBuilder().apply {
      data = REQUISITION_SPEC.toByteString()
    }
      .build()
      .toByteString()
}.build()

@RunWith(JUnit4::class)
class RequistionFulfillmentWorkflowTest {
  private val unfulfilledRequisitionProvider = mock<UnfulfilledRequisitionProvider>()
  private val requisitionDecoder = mock<RequisitionDecoder>()
  private val sketchGenerator = mock<EncryptedSketchGenerator>()
  private val requisitionFulfiller = mock<RequisitionFulfiller>()
  private val requisitionFulfillmentWorkflow =
    RequisitionFulfillmentWorkflow(
      unfulfilledRequisitionProvider,
      requisitionDecoder,
      sketchGenerator,
      requisitionFulfiller
    )

  @Test
  fun noRequisition() = runBlocking {
    whenever(unfulfilledRequisitionProvider.get())
      .thenReturn(null as Requisition?)

    requisitionFulfillmentWorkflow.execute()

    verifyZeroInteractions(
      requisitionDecoder,
      sketchGenerator,
      requisitionFulfiller
    )
  }

  @Test
  fun withRequisition() = runBlocking<Unit> {
    whenever(unfulfilledRequisitionProvider.get())
      .thenReturn(REQUISITION)

    whenever(requisitionDecoder.decodeMeasurementSpec(any()))
      .thenReturn(MEASUREMENT_SPEC)

    whenever(requisitionDecoder.decodeRequisitionSpec(any()))
      .thenReturn(REQUISITION_SPEC)

    whenever(sketchGenerator.generate(any(), any()))
      .thenReturn(flowOf(byteStringOf(0x01), byteStringOf(0x02)))

    lateinit var observedFulfilledRequisitionKey: Requisition.Key
    lateinit var observedFulfilledRequisitionBytes: List<ByteString>
    whenever(requisitionFulfiller.fulfillRequisition(any(), any()))
      .thenAnswer {
        runBlocking {
          observedFulfilledRequisitionKey = it.getArgument<Requisition.Key>(0)
          observedFulfilledRequisitionBytes = it.getArgument<Flow<ByteString>>(1).toList()
        }
      }

    requisitionFulfillmentWorkflow.execute()

    argumentCaptor<Requisition> {
      verifyBlocking(requisitionDecoder) {
        decodeMeasurementSpec(capture())
      }
      assertThat(firstValue).isEqualTo(REQUISITION)
    }

    argumentCaptor<Requisition> {
      verifyBlocking(requisitionDecoder) {
        decodeRequisitionSpec(capture())
      }
      assertThat(firstValue).isEqualTo(REQUISITION)
    }

    val requisitionSpecCaptor = argumentCaptor<RequisitionSpec>()
    val encryptedSketchCaptor = argumentCaptor<EncryptedSketch>()

    verifyBlocking(sketchGenerator) {
      generate(requisitionSpecCaptor.capture(), encryptedSketchCaptor.capture())
    }
    assertThat(requisitionSpecCaptor.firstValue)
      .isEqualTo(REQUISITION_SPEC)
    assertThat(encryptedSketchCaptor.firstValue)
      .isEqualTo(ENCRYPTED_SKETCH)

    assertThat(observedFulfilledRequisitionKey)
      .isEqualTo(REQUISITION.key)

    assertThat(observedFulfilledRequisitionBytes)
      .containsExactly(byteStringOf(0x01), byteStringOf(0x02))
  }
}
