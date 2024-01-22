// Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.UUID
import org.wfanet.panelmatch.client.common.joinKeyIdentifierOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier

@Deprecated(
  "Use a random (not empty) JoinKeyIdentifier instead",
  replaceWith = ReplaceWith("makePaddingQueryJoinKeyIdentifier()"),
)
val PADDING_QUERY_JOIN_KEY_IDENTIFIER = joinKeyIdentifierOf(ByteString.EMPTY)

private val PADDING_QUERY_JOIN_KEY_IDENTIFIER_PREFIX = "padding-query:".toByteStringUtf8()

/** Returns a [JoinKeyIdentifier] suitable for only padding queries. */
fun makePaddingQueryJoinKeyIdentifier(): JoinKeyIdentifier {
  return joinKeyIdentifierOf(
    PADDING_QUERY_JOIN_KEY_IDENTIFIER_PREFIX.concat(UUID.randomUUID().toString().toByteStringUtf8())
  )
}

/** Indicates whether the receiver corresponds to a padding query. */
val JoinKeyIdentifier.isPaddingQuery: Boolean
  get() =
    this == PADDING_QUERY_JOIN_KEY_IDENTIFIER ||
      id.startsWith(PADDING_QUERY_JOIN_KEY_IDENTIFIER_PREFIX)
