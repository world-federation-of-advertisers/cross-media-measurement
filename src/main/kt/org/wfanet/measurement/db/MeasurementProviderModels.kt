package org.wfanet.measurement.db

import java.time.Instant
import org.wfanet.measurement.common.ExternalId

data class DataProviderExternalKey(
  val externalId: ExternalId
)

data class CampaignExternalKey(
  val dataProviderExternalId: ExternalId,
  val externalId: ExternalId
) {
  constructor(dataProviderExternalKey: DataProviderExternalKey, externalId: ExternalId) :
    this(dataProviderExternalKey.externalId, externalId)
}

data class RequisitionExternalKey(
  val dataProviderExternalId: ExternalId,
  val campaignExternalId: ExternalId,
  val externalId: ExternalId
) {
  constructor(campaignExternalKey: CampaignExternalKey, externalId: ExternalId) :
    this(campaignExternalKey.dataProviderExternalId, campaignExternalKey.externalId, externalId)
}

enum class RequisitionState {
  UNFULFILLED,
  FULFILLED,
}

data class Requisition(
  val externalKey: RequisitionExternalKey,
  val windowStartTime: Instant,
  val windowEndTime: Instant,
  val state: RequisitionState
)
