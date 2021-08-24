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

import com.google.common.truth.Truth.assertThat
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.DecryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.GenerateKeysRequest
import org.wfanet.panelmatch.client.privatemembership.ObliviousQueryParameters
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat

private val PLAINTEXTS =
  listOf(
    Pair(1, "<some data a>"),
    Pair(2, "<some data b>"),
    Pair(3, "<some data c>"),
    Pair(4, "<some data d>"),
    Pair(5, "<some data e>")
  )

@RunWith(JUnit4::class)
abstract class AbstractDecryptQueryResultsWorkflowTest : BeamTestBase() {
  abstract val privateMembershipCryptor: PrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper

  private fun runWorkflow(
    privateMembershipCryptor: PrivateMembershipCryptor,
    parameters: Parameters
  ): PCollection<DecryptedQueryResult> {
    val encryptedResults: PCollection<EncryptedQueryResult> =
      encryptedResultOf(privateMembershipCryptorHelper.makeEncryptedResults(PLAINTEXTS))
    return DecryptQueryResultsWorkflow(
        obliviousQueryParameters = parameters,
        privateMembershipCryptor = privateMembershipCryptor
      )
      .batchDecryptQueryResults(encryptedResults)
  }

  @Test
  fun `Decrypt simple set of results`() {
    val obliviousQueryParameters = ObliviousQueryParameters.getDefaultInstance()
    val generateKeysRequest =
      GenerateKeysRequest.newBuilder().setParameters(obliviousQueryParameters).build()
    val generateKeysResponse = privateMembershipCryptor.generateKeys(generateKeysRequest)
    val parameters =
      Parameters(
        obliviousQueryParameters = obliviousQueryParameters,
        privateKey = generateKeysResponse.privateKey,
        publicKey = generateKeysResponse.publicKey
      )
    val decryptedResults = runWorkflow(privateMembershipCryptor, parameters)
    // TODO make sure the query ids line up with the correct data
    assertThat(decryptedResults)
      .containsInAnyOrder(
        plaintextOf("<some data a>"),
        plaintextOf("<some data b>"),
        plaintextOf("<some data c>"),
        plaintextOf("<some data d>"),
        plaintextOf("<some data e>")
      )
  }

  private fun encryptedResultOf(
    entries: List<EncryptedQueryResult>
  ): PCollection<EncryptedQueryResult> {
    return pcollectionOf("Create encryptedResults", *entries.map { it }.toTypedArray())
  }
}
