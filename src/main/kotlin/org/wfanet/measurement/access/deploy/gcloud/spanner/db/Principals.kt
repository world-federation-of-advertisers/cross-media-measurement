/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.access.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundForUserException
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser
import org.wfanet.measurement.internal.access.principal

data class PrincipalResult(val principalId: Long, val principal: Principal)

/** Returns whether the Principal with the specified [principalId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.principalExists(principalId: Long): Boolean {
  return readRow("Principals", Key.of(principalId), listOf("PrincipalId")) != null
}

/**
 * Reads the principal ID by its resource ID.
 *
 * @throws PrincipalNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPrincipalIdByResourceId(
  principalResourceId: String
): Long {
  val struct =
    readRowUsingIndex(
      "Principals",
      "PrincipalsByResourceId",
      Key.of(principalResourceId),
      "PrincipalId",
    ) ?: throw PrincipalNotFoundException(principalResourceId)
  return struct.getLong("PrincipalId")
}

/**
 * Reads the [Principal] by its resource ID.
 *
 * @throws PrincipalNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPrincipalByResourceId(
  principalResourceId: String
): PrincipalResult {
  val sql =
    """
      SELECT
        Principals.*,
        Subject,
        Issuer
      FROM
        Principals
        JOIN UserPrincipals USING (PrincipalId)
      WHERE
        PrincipalResourceId = @principalResourceId
      """
      .trimIndent()
  val struct =
    executeQuery(
        statement(sql) { bind("principalResourceId").to(principalResourceId) },
        Options.tag("action=getPrincipalByResourceId"),
      )
      .singleOrNullIfEmpty() ?: throw PrincipalNotFoundException(principalResourceId)

  return PrincipalResult(
    struct.getLong("PrincipalId"),
    principal {
      this.principalResourceId = principalResourceId
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      user = oAuthUser {
        issuer = struct.getString("Issuer")
        subject = struct.getString("Subject")
      }
    },
  )
}

/**
 * Reads the principal IDs for the specified resource IDs.
 *
 * @throws PrincipalNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPrincipalIdsByResourceIds(
  principalResourceIds: Collection<String>
): Map<String, Long> {
  if (principalResourceIds.isEmpty()) {
    return emptyMap()
  }

  val keys =
    KeySet.newBuilder()
      .apply {
        for (principalResourceId in principalResourceIds) {
          addKey(Key.of(principalResourceId))
        }
      }
      .build()

  val principalIdByResourceId: Map<String, Long> =
    readUsingIndex(
        "Principals",
        "PrincipalsByResourceId",
        keys,
        listOf("PrincipalResourceId", "PrincipalId"),
      )
      .map { it.getString(0) to it.getLong(1) }
      .toList()
      .toMap()

  if (principalIdByResourceId.size != principalResourceIds.size) {
    for (principalResourceId in principalResourceIds) {
      if (!principalIdByResourceId.containsKey(principalResourceId)) {
        throw PrincipalNotFoundException(principalResourceId)
      }
    }
  }

  return principalIdByResourceId
}

/**
 * Reads the [Principal] by its user lookup key.
 *
 * @throws PrincipalNotFoundForUserException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPrincipalByUserKey(
  issuer: String,
  subject: String,
): PrincipalResult {
  val sql =
    """
    SELECT
      Principals.*
    FROM
      Principals
      JOIN UserPrincipals USING (PrincipalId)
    WHERE
      Issuer = @issuer
      AND Subject = @subject
    """
      .trimIndent()
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("issuer").to(issuer)
          bind("subject").to(subject)
        },
        Options.tag("action=getPrincipalByUserKey"),
      )
      .singleOrNullIfEmpty() ?: throw PrincipalNotFoundForUserException(issuer, subject)
  return PrincipalResult(
    row.getLong("PrincipalId"),
    principal {
      principalResourceId = row.getString("PrincipalResourceId")
      user = oAuthUser {
        this.issuer = issuer
        this.subject = subject
      }
      createTime = row.getTimestamp("CreateTime").toProto()
      updateTime = row.getTimestamp("UpdateTime").toProto()
    },
  )
}

/** Buffers an insert mutation to the Principals table. */
fun AsyncDatabaseClient.TransactionContext.insertPrincipal(
  principalId: Long,
  principalResourceId: String,
) {
  bufferInsertMutation("Principals") {
    set("PrincipalId").to(principalId)
    set("PrincipalResourceId").to(principalResourceId)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an insert mutation to the UserPrincipals table. */
fun AsyncDatabaseClient.TransactionContext.insertUserPrincipal(
  principalId: Long,
  issuer: String,
  subject: String,
) {
  bufferInsertMutation("UserPrincipals") {
    set("PrincipalId").to(principalId)
    set("Issuer").to(issuer)
    set("Subject").to(subject)
  }
}

/** Buffers a delete mutation to the Principals table. */
fun AsyncDatabaseClient.TransactionContext.deletePrincipal(principalId: Long) {
  buffer(Mutation.delete("Principals", Key.of(principalId)))
}
