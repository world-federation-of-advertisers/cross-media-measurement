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

package org.wfanet.panelmatch.client.exchangetasks

import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys

fun ApacheBeamContext.buildPrivateMembershipQueries(
  parameters: CreateQueriesParameters,
  privateMembershipCryptor: PrivateMembershipCryptor,
) {
  val lookupKeyAndIds: PCollection<JoinKeyAndId> =
    readBlobAsPCollection("lookup-keys").flatMap {
      JoinKeyAndIdCollection.parseFrom(it).joinKeysAndIdsList
    }

  val hashedJoinKeyAndIds: PCollection<JoinKeyAndId> =
    readBlobAsPCollection("hashed-join-keys").flatMap {
      JoinKeyAndIdCollection.parseFrom(it).joinKeysAndIdsList
    }

  val publicKeyView = readBlobAsView("serialized-rlwe-public-key")

  val privateKeysView =
    readBlobAsPCollection("serialized-rlwe-private-key")
      .mapWithSideInput(publicKeyView, "Make Private Membership Keys") { privateKey, publicKey ->
        AsymmetricKeys(serializedPublicKey = publicKey, serializedPrivateKey = privateKey)
      }
      .toSingletonView()

  val outputs =
    createQueries(
      lookupKeyAndIds,
      hashedJoinKeyAndIds,
      privateKeysView,
      parameters,
      privateMembershipCryptor
    )

  outputs.queryIdMap.write("query-to-join-keys-map")
  outputs.encryptedQueryBundles.write("encrypted-queries")
}
