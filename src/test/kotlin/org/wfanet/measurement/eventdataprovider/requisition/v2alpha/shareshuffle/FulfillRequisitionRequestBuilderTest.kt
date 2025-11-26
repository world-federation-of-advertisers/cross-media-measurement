/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.shareshuffle

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.protocol
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.testing.Requisitions
import org.wfanet.measurement.testing.Requisitions.HMSS_REQUISITION

@RunWith(JUnit4::class)
class FulfillRequisitionRequestBuilderTest {
  @Test
  fun `construction fails when there is no HMSS protocol config`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.build(
          requisition {},
          requisitionNonce = 123L,
          frequencyVector { data += 1 },
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
      }
    assertThat(exception.message).contains("Found: 0")
  }

  @Test
  fun `construction fails when there is more than one HMSS protocol config`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.build(
          requisition {
            protocolConfig = protocolConfig {
              protocols += protocol { honestMajorityShareShuffle = honestMajorityShareShuffle {} }
              protocols += protocol { honestMajorityShareShuffle = honestMajorityShareShuffle {} }
            }
          },
          requisitionNonce = 123L,
          frequencyVector { data += 1 },
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
      }
    assertThat(exception.message).contains("Found: 2")
  }

  @Test
  fun `construction fails when there is not exactly two duchies`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.build(
          HMSS_REQUISITION.copy { duchies.clear() },
          requisitionNonce = 123L,
          frequencyVector { data += 1 },
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
      }
    assertThat(exception.message).contains("Found: 0")
  }

  @Test
  fun `construction fails when there is not exactly one duchy with a public key`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.build(
          HMSS_REQUISITION.copy {
            duchies += duchyEntry {
              this.value = value {
                honestMajorityShareShuffle =
                  DuchyEntryKt.honestMajorityShareShuffle { publicKey = signedMessage {} }
              }
            }
          },
          requisitionNonce = 123L,
          frequencyVector { data += 1 },
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
      }
    assertThat(exception.message).contains("Found: 3")
  }

  @Test
  fun `construction fails when frequency vector is empty`() {
    val exception =
      assertFailsWith<IllegalArgumentException>("expected exception") {
        FulfillRequisitionRequestBuilder.build(
          HMSS_REQUISITION,
          requisitionNonce = 123L,
          frequencyVector {},
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
      }
    assertThat(exception.message).contains("must have size")
  }

  @Test
  fun `build fulfillment requests`() {
    val nonce = 123L
    val inputFrequencyVector = frequencyVector { data += listOf(0, 1, 2, 3, 4) }
    val requisition = HMSS_REQUISITION.copy { this.nonce = nonce }

    val requests =
      FulfillRequisitionRequestBuilder.build(
          requisition,
          nonce,
          inputFrequencyVector,
          Requisitions.DATA_PROVIDER_CERTIFICATE_KEY,
          Requisitions.EDP_SIGNING_KEY,
          "some-etag",
        )
        .toList()

    assertThat(requests.size).isEqualTo(2)
    val firstRequest = requests[0]
    val secondRequest = requests[1]

    assertThat(firstRequest.header.name).isEqualTo(HMSS_REQUISITION.name)
    assertThat(firstRequest.header.requisitionFingerprint)
      .isEqualTo(computeRequisitionFingerprint(HMSS_REQUISITION))
    assertThat(firstRequest.header.nonce).isEqualTo(nonce)
    assertThat(firstRequest.header.honestMajorityShareShuffle.secretSeed.ciphertext.size())
      .isGreaterThan(0)
    assertThat(firstRequest.header.honestMajorityShareShuffle.secretSeed.typeUrl).isNotEmpty()

    assertThat(firstRequest.header.honestMajorityShareShuffle.registerCount).isEqualTo(5)
    assertThat(firstRequest.header.honestMajorityShareShuffle.dataProviderCertificate)
      .isEqualTo(Requisitions.DATA_PROVIDER_CERTIFICATE_KEY.toName())

    assertThat(secondRequest.bodyChunk.data.size())
      .isEqualTo(inputFrequencyVector.toByteString().size())
    // TODO(@kungfucraig): Reconstitute the frequency vector and ensure it's correct
  }
}
