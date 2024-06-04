package org.wfanet.panelmatch.client.common

import com.google.common.hash.Hashing
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.Fingerprinters.farmHashFingerprint64
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.exchangeWorkflow

@RunWith(JUnit4::class)
class FingerprintersTest {

  @Test
  fun farmHashFingerprint64BytesReturnsCorrectHashForRawBytes() {
    val payload = "the quick brown fox".toByteStringUtf8()

    val fingerprint = farmHashFingerprint64(payload)

    assertThat(fingerprint)
      .isEqualTo(
        Hashing.farmHashFingerprint64().hashBytes(payload.toByteArray()).asBytes().toByteString()
      )
  }

  @Test
  fun farmHashFingerprint64BytesReturnsCorrectHashForProtobufMessage() {
    val message = exchangeWorkflow {
      exchangeIdentifiers = exchangeIdentifiers {
        dataProviderId = "some-data-provider"
        modelProviderId = "some-model-provider"
      }
    }

    val fingerprint = message.farmHashFingerprint64()

    assertThat(fingerprint)
      .isEqualTo(
        Hashing.farmHashFingerprint64().hashBytes(message.toByteArray()).asBytes().toByteString()
      )
  }
}
