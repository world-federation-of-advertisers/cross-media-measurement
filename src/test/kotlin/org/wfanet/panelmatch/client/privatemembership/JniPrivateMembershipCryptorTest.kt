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

package org.wfanet.panelmatch.client.privatemembership

import com.google.common.truth.Truth.assertThat
import com.google.privatemembership.batch.ParametersKt.cryptoParameters
import com.google.privatemembership.batch.ParametersKt.shardParameters
import com.google.privatemembership.batch.parameters as clientParameters
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.privatemembership.testing.encryptedQueryOf
import org.wfanet.panelmatch.client.privatemembership.testing.unencryptedQueryOf

@OptIn(kotlin.ExperimentalUnsignedTypes::class)
@RunWith(JUnit4::class)
class JniPrivateMembershipCryptorTest {
  val privateMembershipCryptor = JniPrivateMembershipCryptor()
  val parameters = clientParameters {
    shardParameters =
      shardParameters {
        numberOfShards = 200
        numberOfBucketsPerShard = 2000
      }
    cryptoParameters =
      cryptoParameters {
        logDegree = 12
        logT = 1
        variance = 8
        levelsOfRecursion = 2
        logCompressionFactor = 4
        logDecompositionModulus = 10
        requestModulus += 18446744073708380161UL.toLong()
        requestModulus += 137438953471UL.toLong()
        responseModulus += 2056193UL.toLong()
      }
  }

  @Test
  fun `encryptQueries with multiple shards`() {
    val generateKeysRequest = generateKeysRequest {
      serializedParameters = parameters.toByteString()
    }
    val generateKeysResponse = privateMembershipCryptor.generateKeys(generateKeysRequest)
    val encryptQueriesRequest = privateMembershipEncryptRequest {
      unencryptedQueries +=
        listOf(
          unencryptedQueryOf(100, 1, 1),
          unencryptedQueryOf(100, 2, 2),
          unencryptedQueryOf(101, 3, 1),
          unencryptedQueryOf(101, 4, 5)
        )
      serializedParameters = parameters.toByteString()
      serializedPrivateKey = generateKeysResponse.serializedPrivateKey
      serializedPublicKey = generateKeysResponse.serializedPublicKey
    }
    val encryptedQueries = privateMembershipCryptor.encryptQueries(encryptQueriesRequest)
    assertThat(encryptedQueries.encryptedQueryList)
      .containsExactly(
        encryptedQueryOf(100, 1),
        encryptedQueryOf(100, 2),
        encryptedQueryOf(101, 3),
        encryptedQueryOf(101, 4)
      )
  }

  @Test
  fun `Decrypt Encrypted Result`() {
    // TODO
  }
}
