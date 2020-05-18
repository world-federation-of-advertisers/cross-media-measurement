package org.wfanet.measurement.service.v1alpha.common

import org.wfanet.measurement.api.v1alpha.Campaign
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.ApiId
import org.wfanet.measurement.db.CampaignExternalKey
import org.wfanet.measurement.db.RequisitionExternalKey

/**
 * Converts v1 API proto [Campaign.Key] into an internal API-agnostic representation.
 */
fun Campaign.KeyOrBuilder.toExternalKey(): CampaignExternalKey =
  CampaignExternalKey(ApiId(dataProviderId).externalId,
                              ApiId(campaignId).externalId)

/**
 * Converts v1 API proto [MetricRequisition.Key] into an internal API-agnostic representation.
 */
fun MetricRequisition.KeyOrBuilder.toExternalKey(): RequisitionExternalKey =
  RequisitionExternalKey(ApiId(dataProviderId).externalId,
                                 ApiId(campaignId).externalId,
                                 ApiId(metricRequisitionId).externalId)

/**
 * Converts [RequisitionExternalKey] to a v1 API proto [MetricRequisition.Key].
 */
fun RequisitionExternalKey.toV1Api(): MetricRequisition.Key =
  MetricRequisition.Key.newBuilder().apply {
    dataProviderId = dataProviderExternalId.apiId.value
    campaignId = campaignExternalId.apiId.value
    metricRequisitionId = externalId.apiId.value
  }.build()
