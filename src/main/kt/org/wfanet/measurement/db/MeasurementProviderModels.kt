package org.wfanet.measurement.db

import org.wfanet.measurement.common.ExternalId
import java.time.Instant

data class DataProviderExternalKey (
  val externalId: ExternalId
)

data class CampaignExternalKey (
  val dataProviderExternalId: ExternalId,
  val externalId: ExternalId
)

data class RequisitionExternalKey (
  val dataProviderExternalId: ExternalId,
  val campaignExternalId: ExternalId,
  val externalId: ExternalId
)

// TODO: add states
enum class RequisitionState

data class Requisition (
  val externalKey: RequisitionExternalKey,
  val windowStartTime: Instant,
  val windowEndTime: Instant,
  val state: RequisitionState
)
