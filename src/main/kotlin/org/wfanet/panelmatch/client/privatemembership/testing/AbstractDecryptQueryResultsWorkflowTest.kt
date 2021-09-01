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
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.GenerateKeysRequest
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.ObliviousQueryParameters
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.SymmetricPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.joinKeyOf
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.toByteString

private val PLAINTEXTS =
  listOf(
    plaintextOf("<some long data a>", 1, 6),
    plaintextOf("<some long data b>", 2, 7),
    plaintextOf("<some long data c>", 3, 8),
    plaintextOf("<some long data d>", 4, 9),
    plaintextOf("<some long data e>", 5, 10)
  )
private val JOINKEYS =
  listOf(
    Pair(1, "some joinkey 1"),
    Pair(2, "some joinkey 1"),
    Pair(3, "some joinkey 1"),
    Pair(4, "some joinkey 1"),
    Pair(5, "some joinkey 1")
  )
private val HKDF_PEPPER = "some-pepper".toByteString()

@RunWith(JUnit4::class)
abstract class AbstractDecryptQueryResultsWorkflowTest : BeamTestBase() {
  abstract val symmetricPrivateMembershipCryptor: SymmetricPrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper

  private fun runWorkflow(
    symmetricPrivateMembershipCryptor: SymmetricPrivateMembershipCryptor,
    parameters: Parameters
  ): PCollection<DecryptedEventData> {
    val encryptedEventData: List<EncryptedEventData> =
      privateMembershipCryptorHelper.makeEncryptedEventData(PLAINTEXTS, JOINKEYS)
    val encryptedResults: PCollection<EncryptedQueryResult> =
      encryptedResultOf(
        privateMembershipCryptorHelper.makeEncryptedQueryResults(encryptedEventData)
      )
    val joinkeyCollection = joinkeyCollectionOf(JOINKEYS)
    return DecryptQueryResultsWorkflow(
        obliviousQueryParameters = parameters,
        symmetricPrivateMembershipCryptor = symmetricPrivateMembershipCryptor,
        hkdfPepper = HKDF_PEPPER,
      )
      .batchDecryptQueryResults(
        encryptedQueryResults = encryptedResults,
        queryIdToJoinKey = joinkeyCollection,
      )
  }

  @Test
  fun `Decrypt simple set of results`() {
    val obliviousQueryParameters = ObliviousQueryParameters.getDefaultInstance()
    val generateKeysRequest =
      GenerateKeysRequest.newBuilder().setParameters(obliviousQueryParameters).build()
    val generateKeysResponse =
      symmetricPrivateMembershipCryptor.generatePrivateMembershipKeys(generateKeysRequest)
    val parameters =
      Parameters(
        obliviousQueryParameters = obliviousQueryParameters,
        privateKey = generateKeysResponse.privateKey,
        publicKey = generateKeysResponse.publicKey
      )
    val decryptedResults = runWorkflow(symmetricPrivateMembershipCryptor, parameters)
    assertThat(decryptedResults).containsInAnyOrder(PLAINTEXTS)
  }

  private fun encryptedResultOf(
    entries: List<EncryptedQueryResult>
  ): PCollection<EncryptedQueryResult> {
    return pcollectionOf("Create encryptedResults", *entries.map { it }.toTypedArray())
  }

  private fun joinkeyCollectionOf(
    entries: List<Pair<Int, String>>
  ): PCollection<KV<QueryId, JoinKey>> {
    return pcollectionOf(
      "Create encryptedResults",
      *entries.map { kvOf(queryIdOf(it.first), joinKeyOf(it.second.toByteString())) }.toTypedArray()
    )
  }
}
