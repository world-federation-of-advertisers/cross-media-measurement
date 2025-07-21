/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.eventdataprovider.trustee.v2alpha.FulfillRequisitionRequestBuilder

@RunWith(JUnit4::class)
class FulfillRequisitionRequestBuilderTest {
  @Test
  fun `build fails when requisition has no TrusTee config`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.build(
          requisition { protocolConfig = protocolConfig {} },
          NONCE,
          frequencyVector { data += 1 },
          KMS_CLIENT,
          KEK_URI,
          "idProvider",
          "serviceAccount",
        )
      }
    assertThat(exception.message).contains("Expected to find exactly one config for TrusTee")
    assertThat(exception.message).contains("Found: 0")
  }

  @Test
  fun `build fails when requisition has multiple TrusTee configs`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionRequestBuilder.build(
          REQUISITION.copy {
            protocolConfig =
              protocolConfig.copy {
                protocols += ProtocolConfigKt.protocol { trusTee = ProtocolConfigKt.trusTee {} }
              }
          },
          NONCE,
          frequencyVector { data += 1 },
          KMS_CLIENT,
          KEK_URI,
          "idProvider",
          "serviceAccount",
        )
      }
    assertThat(exception.message).contains("Expected to find exactly one config for TrusTee")
    assertThat(exception.message).contains("Found: 2")
  }

  companion object {
    private val KMS_CLIENT = FakeKmsClient()
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "key1"
    private const val NONCE = 12345L
    private val REQUISITION = requisition {
      name = "requisitions/test"
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol { trusTee = ProtocolConfigKt.trusTee {} }
      }
    }
  }
}
