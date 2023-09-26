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

import com.google.privatemembership.batch.ParametersKt.shardParameters
import com.google.privatemembership.batch.Shared
import com.google.privatemembership.batch.Shared.PublicKey
import com.google.privatemembership.batch.client.Client.PrivateKey
import com.google.privatemembership.batch.client.generateKeysRequest
import com.google.privatemembership.batch.parameters
import java.io.Serializable
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembership

class JniQueryEvaluatorContext(shardCount: Int, bucketsPerShardCount: Int) : Serializable {
  val privateMembershipParameters: Shared.Parameters = parameters {
    shardParameters = shardParameters {
      numberOfShards = shardCount
      numberOfBucketsPerShard = bucketsPerShardCount + 1 // To account for padding queries
      enablePaddingNonces = true
    }
    cryptoParameters = PRIVATE_MEMBERSHIP_CRYPTO_PARAMETERS
  }

  val privateMembershipPublicKey: PublicKey
  val privateMembershipPrivateKey: PrivateKey

  init {
    val response =
      JniPrivateMembership.generateKeys(
        generateKeysRequest { parameters = privateMembershipParameters }
      )
    privateMembershipPublicKey = response.publicKey
    privateMembershipPrivateKey = response.privateKey
  }
}
