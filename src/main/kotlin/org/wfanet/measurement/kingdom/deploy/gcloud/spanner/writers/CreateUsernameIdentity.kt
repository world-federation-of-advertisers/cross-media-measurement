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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set

internal fun SpannerWriter.TransactionScope.createUsernameIdentity(
  accountId: InternalId,
  username: String,
  password: String
) {
  val internalUsernameIdentityId = idGenerator.generateInternalId()
  val salt = stringGenerator.generateString()
  val encryptedPassword = hashSha256(ByteString.copyFromUtf8(password + salt)).toString()

  transactionContext.bufferInsertMutation("UsernameIdentities") {
    set("UsernameIdentityId" to internalUsernameIdentityId)
    set("AccountId" to accountId)
    set("Username" to username)
    set("Password" to encryptedPassword)
    set("Salt" to salt)
  }
}
