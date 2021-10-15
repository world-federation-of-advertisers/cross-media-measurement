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

package org.wfanet.panelmatch.client.deploy

import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapperForJoinKeyExchange
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.JniQueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.compression.BrotliCompressorFactory
import org.wfanet.panelmatch.common.crypto.JniDeterministicCommutativeCipher

class ProductionExchangeTaskMapper(
  override val privateStorage: StorageFactory,
  override val inputTaskThrottler: Throttler,
) : ExchangeTaskMapperForJoinKeyExchange() {
  override val compressorFactory by lazy { BrotliCompressorFactory() }
  override val deterministicCommutativeCryptor by lazy { JniDeterministicCommutativeCipher() }
  override val getPrivateMembershipCryptor = ::JniPrivateMembershipCryptor
  override val getQueryResultsEvaluator = ::JniQueryEvaluator
  override val queryResultsDecryptor by lazy { JniQueryResultsDecryptor() }
}
