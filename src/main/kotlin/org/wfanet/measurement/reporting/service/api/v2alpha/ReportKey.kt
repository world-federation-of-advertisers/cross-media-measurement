package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser("measurementConsumers/{measurement_consumer}/reports/{report}")

/** [ResourceKey] of a Report. */
data class ReportKey(
  val measurementConsumerId: String,
  val reportId: String,
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId,
        IdVariable.REPORT to reportId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ReportKey> {
    val defaultValue = ReportKey("", "")

    override fun fromName(resourceName: String): ReportKey? {
      return parser.parseIdVars(resourceName)?.let {
        ReportKey(it.getValue(IdVariable.MEASUREMENT_CONSUMER), it.getValue(IdVariable.REPORT))
      }
    }
  }
}
