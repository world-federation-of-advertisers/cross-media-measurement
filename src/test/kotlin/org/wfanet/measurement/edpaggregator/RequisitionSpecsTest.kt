/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData

@RunWith(JUnit4::class)
class RequisitionSpecsTest {
  @Test
  fun `reads signed and encrypted specs of a non-multimeasurement Requisition`() {
    val requisition = TestRequisitionData.REQUISITION

    assertThat(requisition.isMultimeasurement).isFalse()
    assertThat(requisition.readMeasurementSpec()).isEqualTo(TestRequisitionData.MEASUREMENT_SPEC)
    assertThat(requisition.readRequisitionSpec(PRIVATE_ENCRYPTION_KEY))
      .isEqualTo(TestRequisitionData.REQUISITION_SPEC)
  }

  @Test
  fun `reads unsigned and plaintext specs of a multimeasurement Requisition`() {
    assertThat(MULTI_REQUISITION.isMultimeasurement).isTrue()
    assertThat(MULTI_REQUISITION.readMeasurementSpec()).isEqualTo(MULTI_MEASUREMENT_SPEC)
    assertThat(MULTI_REQUISITION.readRequisitionSpec(PRIVATE_ENCRYPTION_KEY))
      .isEqualTo(MULTI_REQUISITION_SPEC)
  }

  @Test
  fun `readMeasurementSpec throws for an unsigned spec with a non-TrusTEE v2 protocol`() {
    val requisition =
      MULTI_REQUISITION.copy { protocolConfig = TestRequisitionData.REQUISITION.protocolConfig }

    assertFailsWith<IllegalArgumentException> { requisition.readMeasurementSpec() }
  }

  @Test
  fun `readMeasurementSpec throws for an unsigned spec that is not a multimeasurement`() {
    val requisition =
      MULTI_REQUISITION.copy { unsignedMeasurementSpec = TestRequisitionData.MEASUREMENT_SPEC }

    assertFailsWith<IllegalArgumentException> { requisition.readMeasurementSpec() }
  }

  @Test
  fun `readMeasurementSpec throws when both signed and unsigned specs are set`() {
    val requisition =
      MULTI_REQUISITION.copy { measurementSpec = TestRequisitionData.REQUISITION.measurementSpec }

    assertFailsWith<IllegalArgumentException> { requisition.readMeasurementSpec() }
  }

  @Test
  fun `readRequisitionSpec throws for a multimeasurement without a plaintext spec`() {
    val requisition = MULTI_REQUISITION.copy { clearRequisitionSpec() }

    assertFailsWith<IllegalArgumentException> {
      requisition.readRequisitionSpec(PRIVATE_ENCRYPTION_KEY)
    }
  }

  @Test
  fun `readRequisitionSpec throws for a multimeasurement with an encrypted spec`() {
    val requisition =
      MULTI_REQUISITION.copy {
        encryptedRequisitionSpec = TestRequisitionData.REQUISITION.encryptedRequisitionSpec
      }

    assertFailsWith<IllegalArgumentException> {
      requisition.readRequisitionSpec(PRIVATE_ENCRYPTION_KEY)
    }
  }

  companion object {
    private val PRIVATE_ENCRYPTION_KEY = TestRequisitionData.EDP_DATA.privateEncryptionKey

    private val MULTI_REQUISITION_SPEC: RequisitionSpec =
      TestRequisitionData.REQUISITION_SPEC.copy { clearNonce() }

    private val MULTI_MEASUREMENT_SPEC: MeasurementSpec =
      TestRequisitionData.MEASUREMENT_SPEC.copy {
        nonceHashes.clear()
        multi = MeasurementSpecKt.multiMeasurementSpec {}
      }

    private val MULTI_REQUISITION: Requisition =
      TestRequisitionData.REQUISITION.copy {
        clearMeasurementSpec()
        clearEncryptedRequisitionSpec()
        unsignedMeasurementSpec = MULTI_MEASUREMENT_SPEC
        requisitionSpec = MULTI_REQUISITION_SPEC
        protocolConfig = protocolConfig {
          protocols += ProtocolConfigKt.protocol { trusTeeV2 = ProtocolConfigKt.trusTeeV2 {} }
        }
      }
  }
}
