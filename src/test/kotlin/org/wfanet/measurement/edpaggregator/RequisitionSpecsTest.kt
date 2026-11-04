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
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
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

    assertThat(RequisitionSpecs.isMultimeasurement(requisition)).isFalse()
    assertThat(RequisitionSpecs.readMeasurementSpec(requisition))
      .isEqualTo(TestRequisitionData.MEASUREMENT_SPEC)
    assertThat(RequisitionSpecs.readRequisitionSpec(requisition, PRIVATE_ENCRYPTION_KEY))
      .isEqualTo(TestRequisitionData.REQUISITION_SPEC)
  }

  @Test
  fun `reads unsigned and plaintext specs of a multimeasurement Requisition`() {
    assertThat(RequisitionSpecs.isMultimeasurement(MULTI_REQUISITION)).isTrue()
    assertThat(RequisitionSpecs.readMeasurementSpec(MULTI_REQUISITION))
      .isEqualTo(MULTI_MEASUREMENT_SPEC)
    assertThat(RequisitionSpecs.readRequisitionSpec(MULTI_REQUISITION, PRIVATE_ENCRYPTION_KEY))
      .isEqualTo(MULTI_REQUISITION_SPEC)
  }

  @Test
  fun `refuses a MULTI Requisition with a protocol other than TrusTEE v2`() {
    assertRefused(
      MULTI_REQUISITION.copy {
        protocolConfig =
          TestRequisitionData.REQUISITION.protocolConfig.copy {
            measurementType = ProtocolConfig.MeasurementType.MULTI
          }
      }
    )
  }

  @Test
  fun `refuses a MULTI Requisition whose MeasurementSpec is not a MultiMeasurementSpec`() {
    assertRefused(
      MULTI_REQUISITION.copy { unsignedMeasurementSpec = TestRequisitionData.MEASUREMENT_SPEC }
    )
  }

  @Test
  fun `refuses a MULTI Requisition that also carries a signed MeasurementSpec`() {
    assertRefused(
      MULTI_REQUISITION.copy { measurementSpec = TestRequisitionData.REQUISITION.measurementSpec }
    )
  }

  @Test
  fun `refuses a MULTI Requisition without a plaintext RequisitionSpec`() {
    assertRefused(MULTI_REQUISITION.copy { clearRequisitionSpec() })
  }

  @Test
  fun `refuses a MULTI Requisition that also carries an encrypted RequisitionSpec`() {
    assertRefused(
      MULTI_REQUISITION.copy {
        encryptedRequisitionSpec = TestRequisitionData.REQUISITION.encryptedRequisitionSpec
      }
    )
  }

  @Test
  fun `refuses a MULTI Requisition with signed and encrypted specs`() {
    assertRefused(TestRequisitionData.REQUISITION.copy { protocolConfig = MULTI_PROTOCOL_CONFIG })
  }

  @Test
  fun `refuses an unsigned and plaintext Requisition whose measurement type is not MULTI`() {
    assertRefused(
      MULTI_REQUISITION.copy {
        protocolConfig =
          MULTI_PROTOCOL_CONFIG.copy {
            measurementType = ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
          }
      }
    )
  }

  /** Asserts that both readers refuse [requisition]. */
  private fun assertRefused(requisition: Requisition) {
    assertFailsWith<IllegalArgumentException> { RequisitionSpecs.readMeasurementSpec(requisition) }
    assertFailsWith<IllegalArgumentException> {
      RequisitionSpecs.readRequisitionSpec(requisition, PRIVATE_ENCRYPTION_KEY)
    }
  }

  companion object {
    private val PRIVATE_ENCRYPTION_KEY = TestRequisitionData.EDP_DATA.privateEncryptionKey

    private val MULTI_PROTOCOL_CONFIG = protocolConfig {
      measurementType = ProtocolConfig.MeasurementType.MULTI
      protocols += ProtocolConfigKt.protocol { trusTeeV2 = ProtocolConfigKt.trusTeeV2 {} }
    }

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
        protocolConfig = MULTI_PROTOCOL_CONFIG
      }
  }
}
