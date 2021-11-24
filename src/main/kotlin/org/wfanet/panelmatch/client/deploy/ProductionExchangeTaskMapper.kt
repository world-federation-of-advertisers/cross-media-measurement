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

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapperForJoinKeyExchange
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.JniQueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.crypto.JniDeterministicCommutativeCipher

class ProductionExchangeTaskMapper(
  override val inputTaskThrottler: Throttler,
  override val privateStorageSelector: PrivateStorageSelector,
  override val sharedStorageSelector: SharedStorageSelector,
  override val certificateManager: CertificateManager,
  private val pipelineOptions: PipelineOptions
) : ExchangeTaskMapperForJoinKeyExchange() {
  override val deterministicCommutativeCryptor by lazy { JniDeterministicCommutativeCipher() }
  override val getPrivateMembershipCryptor = ::JniPrivateMembershipCryptor
  override val getQueryResultsEvaluator = ::JniQueryEvaluator
  override val queryResultsDecryptor by lazy { JniQueryResultsDecryptor() }
  override fun newPipeline(): Pipeline = Pipeline.create(pipelineOptions)
}
