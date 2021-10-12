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

package org.wfanet.panelmatch.client.privatemembership.testing

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.testing.FakeCompressorFactory
import org.wfanet.panelmatch.client.common.testing.FakeDictionaryBuilder
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class PlaintextDecryptQueryResultsTest : AbstractDecryptQueryResultsTest() {
  override val privateMembershipSerializedParameters = "some serialized parameters".toByteString()
  override val queryResultsDecryptor = PlaintextQueryResultsDecryptor()
  override val privateMembershipCryptor =
    PlaintextPrivateMembershipCryptor(privateMembershipSerializedParameters)
  override val privateMembershipCryptorHelper = PlaintextPrivateMembershipCryptorHelper()
  override val dictionaryBuilder = FakeDictionaryBuilder()
  override val compressorFactory = FakeCompressorFactory()
}
