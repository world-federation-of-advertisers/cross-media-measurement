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

package org.wfanet.panelmatch.common.crypto.testing

import com.google.protobuf.ByteString
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher

@RunWith(JUnit4::class)
class FakeDeterministicCommutativeCipherTest : AbstractDeterministicCommutativeCipherTest() {
  override val cipher: DeterministicCommutativeCipher = FakeDeterministicCommutativeCipher
  override val invalidKey: ByteString = FakeDeterministicCommutativeCipher.INVALID_KEY
  override val privateKey1 = cipher.generateKey()
  override val privateKey2 = cipher.generateKey()
}
