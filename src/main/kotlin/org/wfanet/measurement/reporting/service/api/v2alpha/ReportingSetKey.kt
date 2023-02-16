package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser("measurementConsumers/{measurement_consumer}/reportingSets/{reporting_set}")

/** [ResourceKey] of a ReportingSet. */
data class ReportingSetKey(
  val measurementConsumerId: String,
  val reportingSetId: String,
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId,
        IdVariable.REPORTING_SET to reportingSetId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ReportingSetKey> {
    val defaultValue = ReportingSetKey("", "")

    override fun fromName(resourceName: String): ReportingSetKey? {
      return parser.parseIdVars(resourceName)?.let {
        ReportingSetKey(
          it.getValue(IdVariable.MEASUREMENT_CONSUMER),
          it.getValue(IdVariable.REPORTING_SET)
        )
      }
    }
  }
}
