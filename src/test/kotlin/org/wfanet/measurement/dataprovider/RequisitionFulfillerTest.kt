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
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle

@RunWith(JUnit4::class)
class RequisitionFulfillerTest {
  @Test
  fun `encryptionPublicKeysMatch returns true for a different serialization of the same key`() {
    assertThat(encryptionPublicKeysMatch(STORED_PUBLIC_KEY, LIVE_PUBLIC_KEY)).isTrue()
  }

  @Test
  fun `encryptionPublicKeysMatch returns true when both sides are the same live serialization`() {
    assertThat(encryptionPublicKeysMatch(LIVE_PUBLIC_KEY, LIVE_PUBLIC_KEY)).isTrue()
  }

  @Test
  fun `encryptionPublicKeysMatch returns false for a different key`() {
    assertThat(encryptionPublicKeysMatch(STORED_PUBLIC_KEY, OTHER_LIVE_PUBLIC_KEY)).isFalse()
  }

  companion object {
    private val SECRET_FILES_PATH =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    /**
     * Raw bytes of a Tink keyset as they'd be read back from long-term storage (e.g. the Kingdom's
     * DataProvider registration), without having been re-serialized by the currently-running Tink
     * binary.
     */
    private val STORED_PUBLIC_KEY_BYTES: ByteString =
      SECRET_FILES_PATH.resolve("edp1_enc_public.tink").toFile().readByteString()

    private val STORED_PUBLIC_KEY: EncryptionPublicKey = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = STORED_PUBLIC_KEY_BYTES
    }

    /** The same cryptographic key as [STORED_PUBLIC_KEY], serialized live by this Tink binary. */
    private val LIVE_PUBLIC_KEY: EncryptionPublicKey =
      STORED_PUBLIC_KEY.toPublicKeyHandle().toEncryptionPublicKey().also {
        check(it.data != STORED_PUBLIC_KEY.data) {
          "Expected a different serialization for this test to be meaningful"
        }
      }

    private val OTHER_LIVE_PUBLIC_KEY: EncryptionPublicKey =
      SECRET_FILES_PATH.resolve("edp2_enc_public.tink")
        .toFile()
        .readByteString()
        .let { bytes ->
          encryptionPublicKey {
            format = EncryptionPublicKey.Format.TINK_KEYSET
            data = bytes
          }
        }
        .toPublicKeyHandle()
        .toEncryptionPublicKey()
  }
}
