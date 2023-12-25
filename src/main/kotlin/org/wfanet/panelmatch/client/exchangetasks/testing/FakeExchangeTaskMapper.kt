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

package org.wfanet.panelmatch.client.exchangetasks.testing

import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper

class FakeExchangeTaskMapper(
  private val createExchangeTask: (String) -> ExchangeTask = ::FakeExchangeTask
) : ExchangeTaskMapper() {
  override suspend fun ExchangeContext.commutativeDeterministicEncrypt() =
    createExchangeTask("commutative-deterministic-encrypt")

  override suspend fun ExchangeContext.commutativeDeterministicDecrypt() =
    createExchangeTask("commutative-deterministic-decrypt")

  override suspend fun ExchangeContext.commutativeDeterministicReEncrypt() =
    createExchangeTask("commutative-deterministic-re-encrypt")

  override suspend fun ExchangeContext.generateCommutativeDeterministicEncryptionKey() =
    createExchangeTask("generate-commutative-deterministic-encryption-key")

  override suspend fun ExchangeContext.preprocessEvents() = createExchangeTask("preprocess-events")

  override suspend fun ExchangeContext.buildPrivateMembershipQueries() =
    createExchangeTask("build-private-membership-queries")

  override suspend fun ExchangeContext.executePrivateMembershipQueries() =
    createExchangeTask("execute-private-membership-queries")

  override suspend fun ExchangeContext.decryptMembershipResults() =
    createExchangeTask("decrypt-membership-results")

  override suspend fun ExchangeContext.generateSerializedRlweKeyPair() =
    createExchangeTask("generate-serialized-rlwe-key-pair")

  override suspend fun ExchangeContext.generateExchangeCertificate() =
    createExchangeTask("generate-exchange-certificate")

  override suspend fun ExchangeContext.generateLookupKeys() =
    createExchangeTask("generate-lookup-keys")

  override suspend fun ExchangeContext.intersectAndValidate() =
    createExchangeTask("intersect-and-validate")

  override suspend fun ExchangeContext.input() = createExchangeTask("input")

  override suspend fun ExchangeContext.copyFromPreviousExchange() =
    createExchangeTask("copy-from-previous-exchange")

  override suspend fun ExchangeContext.copyFromSharedStorage() =
    createExchangeTask("copy-from-shared-storage")

  override suspend fun ExchangeContext.copyToSharedStorage() =
    createExchangeTask("copy-to-shared-storage")

  override suspend fun ExchangeContext.hybridEncrypt() = createExchangeTask("hybrid-encrypt")

  override suspend fun ExchangeContext.hybridDecrypt() = createExchangeTask("hybrid-decrypt")

  override suspend fun ExchangeContext.generateHybridEncryptionKeyPair() =
    createExchangeTask("generate-hybrid-encryption-key-pair")

  override suspend fun ExchangeContext.generateRandomBytes() =
    createExchangeTask("generate-random-bytes")

  override suspend fun ExchangeContext.assignJoinKeyIds() =
    createExchangeTask("assign-join-key-ids")
}
