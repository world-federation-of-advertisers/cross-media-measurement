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
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper

class FakeExchangeTaskMapper : ExchangeTaskMapper() {
  override suspend fun ExchangeContext.encrypt() = FakeExchangeTask("encrypt")

  override suspend fun ExchangeContext.decrypt() = FakeExchangeTask("decrypt")

  override suspend fun ExchangeContext.reEncrypt() = FakeExchangeTask("re-encrypt")

  override suspend fun ExchangeContext.generateEncryptionKey() =
    FakeExchangeTask("generate-encryption-key")

  override suspend fun ExchangeContext.buildPrivateMembershipQueries() =
    FakeExchangeTask("build-private-membership-queries")

  override suspend fun ExchangeContext.executePrivateMembershipQueries() =
    FakeExchangeTask("execute-private-membership-queries")

  override suspend fun ExchangeContext.decryptMembershipResults() =
    FakeExchangeTask("decrypt-membership-results")

  override suspend fun ExchangeContext.generateSymmetricKey() =
    FakeExchangeTask("generate-symmetric-key")

  override suspend fun ExchangeContext.generateSerializedRlweKeys() =
    FakeExchangeTask("generate-serialized-rlwe-keys")

  override suspend fun ExchangeContext.generateExchangeCertificate() =
    FakeExchangeTask("generate-exchange-certificate")

  override suspend fun ExchangeContext.generateLookupKeys() =
    FakeExchangeTask("generate-lookup-keys")

  override suspend fun ExchangeContext.intersectAndValidate() =
    FakeExchangeTask("intersect-and-validate")

  override suspend fun ExchangeContext.input() = FakeExchangeTask("input")

  override suspend fun ExchangeContext.copyFromPreviousExchange() =
    FakeExchangeTask("copy-from-previous-exchange")

  override suspend fun ExchangeContext.copyFromSharedStorage() =
    FakeExchangeTask("copy-from-shared-storage")

  override suspend fun ExchangeContext.copyToSharedStorage() =
    FakeExchangeTask("copy-to-shared-storage")
}
