/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import java.lang.Exception
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.SecretShareGeneratorAdapter

@RunWith(JUnit4::class)
class NativeTest {
  @Test
  fun `loadLibrary loads library`() {
    // Verify that we do not get an UnsatisfiedLinkError.
    try {
      SecretShareGeneratorAdapter.generateSecretShares(byteArrayOf())
    } catch (e: Exception) {
      // No-op.
    }
  }

  companion object {
    init {
      Native.loadLibrary()
    }
  }
}
