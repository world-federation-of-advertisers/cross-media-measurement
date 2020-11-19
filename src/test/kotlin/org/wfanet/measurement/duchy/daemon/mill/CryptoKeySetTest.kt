// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.daemon.mill

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.common.crypto.ElGamalKeyPair

class CryptoKeySetTest {

  @Test
  fun `convert valid string to elgamal key`() {
    val elGamalKeyPair: ElGamalKeyPair =
      "$PUBLIC_KEY_G$PUBLIC_KEY_Y$PRIVATE_KEY".toElGamalKeyPair()
    assertEquals(elGamalKeyPair.publicKey.generator.size(), 33)
    assertEquals(elGamalKeyPair.publicKey.element.size(), 33)
    assertEquals(elGamalKeyPair.secretKey.size(), 32)
  }

  @Test
  fun `wrong size should fail`() {
    assertFailsWith(IllegalArgumentException::class) { "123445".toElGamalKeyPair() }
  }

  companion object {
    private const val PUBLIC_KEY_G =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
    private const val PUBLIC_KEY_Y =
      "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3"
    private const val PRIVATE_KEY =
      "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
  }
}
