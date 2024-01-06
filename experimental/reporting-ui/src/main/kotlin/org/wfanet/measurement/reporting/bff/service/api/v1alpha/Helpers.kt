package org.wfanet.measurement.reporting.bff.service.api.v1alpha

class Frequency(
    val label: String,
    val value: Double,
){}

class Helper(
    val reportingSet: String,
    val groups: List<String>,
    val start: Long,
    val end: Long,
    val reach: Long?,
    val frequencies: List<Frequency>?,
    val impression: Long?
) {

}
