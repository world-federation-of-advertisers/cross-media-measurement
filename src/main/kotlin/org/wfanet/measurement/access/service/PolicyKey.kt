package org.wfanet.measurement.access.service

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

class PolicyKey(val policyId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(mapOf(IdVariable.POLICY to policyId))
  }

  companion object : ResourceKey.Factory<PolicyKey> {
    private val parser = ResourceNameParser("policies/{policy}")

    override fun fromName(resourceName: String): PolicyKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return PolicyKey(idVars.getValue(IdVariable.POLICY))
    }
  }
}
