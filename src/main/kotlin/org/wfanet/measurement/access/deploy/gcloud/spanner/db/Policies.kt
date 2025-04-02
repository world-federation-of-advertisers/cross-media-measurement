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
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.access.service.internal.PolicyNotFoundException
import org.wfanet.measurement.access.service.internal.PolicyNotFoundForProtectedResourceException
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getNullableString
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.access.Policy
import org.wfanet.measurement.internal.access.PolicyKt.members
import org.wfanet.measurement.internal.access.policy

data class PolicyResult(val policyId: Long, val policy: Policy)

/** Returns whether the [Policy] with the specified [policyId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.policyExists(policyId: Long): Boolean {
  return readRow("Policies", Key.of(policyId), listOf("PolicyId")) != null
}

/**
 * Reads a [Policy] by its [policyResourceId].
 *
 * @throws PolicyNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPolicyByResourceId(
  policyResourceId: String
): PolicyResult {
  val sql = buildString {
    appendLine(Policies.BASE_SQL)
    appendLine("WHERE PolicyResourceId = @policyResourceId")
  }
  val rows: List<Struct> =
    executeQuery(
        statement(sql) { bind("policyResourceId").to(policyResourceId) },
        Options.tag("action=getPolicyByResourceId"),
      )
      .toList()

  if (rows.isEmpty()) {
    throw PolicyNotFoundException(policyResourceId)
  }

  return Policies.buildPolicyResult(rows)
}

/**
 * Reads a [Policy] by its [protectedResourceName].
 *
 * @throws PolicyNotFoundForProtectedResourceException
 */
suspend fun AsyncDatabaseClient.ReadContext.getPolicyByProtectedResourceName(
  protectedResourceName: String
): PolicyResult {
  val sql = buildString {
    appendLine(Policies.BASE_SQL)
    appendLine("WHERE ProtectedResourceName = @protectedResourceName")
  }
  val rows: List<Struct> =
    executeQuery(
        statement(sql) { bind("protectedResourceName").to(protectedResourceName) },
        Options.tag("action=getPolicyByProtectedResourceName"),
      )
      .toList()

  if (rows.isEmpty()) {
    throw PolicyNotFoundForProtectedResourceException(protectedResourceName)
  }

  return Policies.buildPolicyResult(rows)
}

/** Buffers an insert mutation for the Policies table. */
fun AsyncDatabaseClient.TransactionContext.insertPolicy(
  policyId: Long,
  policyResourceId: String,
  protectedResourceName: String,
) {
  bufferInsertMutation("Policies") {
    set("PolicyId").to(policyId)
    set("PolicyResourceId").to(policyResourceId)
    set("ProtectedResourceName").to(protectedResourceName)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update mutation for the Policies table. */
fun AsyncDatabaseClient.TransactionContext.updatePolicy(policyId: Long) {
  bufferUpdateMutation("Policies") {
    set("PolicyId").to(policyId)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an insert mutation for the PolicyBindings table. */
fun AsyncDatabaseClient.TransactionContext.insertPolicyBinding(
  policyId: Long,
  roleId: Long,
  principalId: Long,
) {
  bufferInsertMutation("PolicyBindings") {
    set("PolicyId").to(policyId)
    set("RoleId").to(roleId)
    set("PrincipalId").to(principalId)
  }
}

/** Buffers a delete mutation for the PolicyBindings table. */
fun AsyncDatabaseClient.TransactionContext.deletePolicyBinding(
  policyId: Long,
  roleId: Long,
  principalId: Long,
) {
  buffer(Mutation.delete("PolicyBindings", Key.of(policyId, roleId, principalId)))
}

private object Policies {
  val BASE_SQL =
    """
    SELECT
      Policies.*,
      RoleResourceId,
      PrincipalResourceId
    FROM
      Policies
      LEFT JOIN (
        PolicyBindings
        JOIN Roles USING (RoleId)
        LEFT JOIN Principals USING (PrincipalId)
      )  USING (PolicyId)
    """
      .trimIndent()

  fun buildPolicyResult(rows: Iterable<Struct>): PolicyResult {
    val firstRow = rows.first()
    val membersByRole: Map<String, List<String>> = buildMembersByRole(rows)
    return PolicyResult(
      firstRow.getLong("PolicyId"),
      policy {
        policyResourceId = firstRow.getString("PolicyResourceId")
        protectedResourceName = firstRow.getString("ProtectedResourceName")
        createTime = firstRow.getTimestamp("CreateTime").toProto()
        updateTime = firstRow.getTimestamp("UpdateTime").toProto()
        etag = ETags.computeETag(updateTime.toInstant())

        for ((roleResourceId, memberPrincipalResourceIds) in membersByRole) {
          bindings[roleResourceId] = members {
            this.memberPrincipalResourceIds += memberPrincipalResourceIds
          }
        }
      },
    )
  }

  private fun buildMembersByRole(rows: Iterable<Struct>): Map<String, List<String>> {
    return mutableMapOf<String, MutableList<String>>().apply {
      for (row in rows) {
        val roleResourceId: String = row.getNullableString("RoleResourceId") ?: continue
        val principalResourceId: String = row.getNullableString("PrincipalResourceId") ?: continue
        val memberPrincipalResourceIds: MutableList<String> =
          getOrPut(roleResourceId, ::mutableListOf)
        memberPrincipalResourceIds.add(principalResourceId)
      }
    }
  }
}
