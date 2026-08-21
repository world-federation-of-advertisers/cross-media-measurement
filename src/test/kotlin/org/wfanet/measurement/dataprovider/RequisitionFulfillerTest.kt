// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle

@RunWith(JUnit4::class)
class RequisitionFulfillerTest {
  @Test
  fun `encryptionPublicKeysMatch returns true for a different serialization of the same key`() {
    assertThat(encryptionPublicKeysMatch(STORED_PUBLIC_KEY, STORED_PUBLIC_KEY_HANDLE)).isTrue()
  }

  @Test
  fun `encryptionPublicKeysMatch returns true when the expected side holds the stale serialization`() {
    // Regression test: encryptionPublicKeysMatch must canonicalize both sides, not just the
    // candidate. Here the candidate is already in its live-serialized form and the expected side
    // wraps the stale stored bytes -- the opposite arrangement from the test above.
    assertThat(encryptionPublicKeysMatch(LIVE_PUBLIC_KEY, STORED_PUBLIC_KEY_HANDLE)).isTrue()
  }

  @Test
  fun `encryptionPublicKeysMatch returns false for a different key`() {
    assertThat(encryptionPublicKeysMatch(STORED_PUBLIC_KEY, OTHER_PUBLIC_KEY_HANDLE)).isFalse()
  }

  @Test
  fun `encryptionPublicKeysMatch returns false for an unparseable key rather than throwing`() {
    val unparseableKey = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = ByteString.copyFromUtf8("not a valid Tink keyset")
    }

    assertThat(encryptionPublicKeysMatch(unparseableKey, STORED_PUBLIC_KEY_HANDLE)).isFalse()
  }

  @Test
  fun `encryptionPublicKeysMatch returns false for an unspecified format rather than throwing`() {
    val unspecifiedFormatKey = encryptionPublicKey { data = STORED_PUBLIC_KEY.data }

    assertThat(encryptionPublicKeysMatch(unspecifiedFormatKey, STORED_PUBLIC_KEY_HANDLE)).isFalse()
  }

  companion object {
    private val SECRET_FILES_PATH =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    private fun readPublicKey(fileName: String): EncryptionPublicKey = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRET_FILES_PATH.resolve(fileName).toFile().readByteString()
    }

    /**
     * Raw bytes of a Tink keyset as they'd be read back from long-term storage (e.g. the Kingdom's
     * DataProvider registration), without having been re-serialized by the currently-running Tink
     * binary.
     */
    private val STORED_PUBLIC_KEY: EncryptionPublicKey = readPublicKey("edp1_enc_public.tink")

    private val STORED_PUBLIC_KEY_HANDLE: PublicKeyHandle = STORED_PUBLIC_KEY.toPublicKeyHandle()

    /** The same cryptographic key as [STORED_PUBLIC_KEY], serialized live by this Tink binary. */
    private val LIVE_PUBLIC_KEY: EncryptionPublicKey =
      STORED_PUBLIC_KEY_HANDLE.toEncryptionPublicKey().also {
        check(it.data != STORED_PUBLIC_KEY.data) {
          "Expected a different serialization for this test to be meaningful"
        }
      }

    private val OTHER_PUBLIC_KEY_HANDLE: PublicKeyHandle =
      readPublicKey("edp2_enc_public.tink").toPublicKeyHandle()
  }
}
