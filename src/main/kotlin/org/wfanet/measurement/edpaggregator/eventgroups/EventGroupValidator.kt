// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup

/** Validates an EDP's EventGroup before it is synchronized with the CMMS public API. */
object EventGroupValidator {
  /**
   * Validates [eventGroup].
   *
   * @throws IllegalStateException if a required field is missing or invalid
   */
  fun validate(eventGroup: EventGroup) {
    check(eventGroup.state != EventGroup.State.UNRECOGNIZED) {
      "state must be STATE_UNSPECIFIED, ACTIVE, or DELETED"
    }
    if (eventGroup.state == EventGroup.State.DELETED) {
      validateDeleted(eventGroup)
      return
    }

    check(eventGroup.mediaTypesCount > 0) { "At least one media type must be set" }
    check(EventGroup.MediaType.MEDIA_TYPE_UNSPECIFIED !in eventGroup.mediaTypesList) {
      "media_types must not contain MEDIA_TYPE_UNSPECIFIED"
    }
    check(EventGroup.MediaType.UNRECOGNIZED !in eventGroup.mediaTypesList) {
      "media_types contains an unrecognized value"
    }

    check(eventGroup.hasDataAvailabilityInterval()) { "Data availability must be set" }
    val interval = eventGroup.dataAvailabilityInterval
    check(interval.hasStartTime() && Timestamps.isValid(interval.startTime)) {
      "data_availability_interval.start_time must be a valid timestamp"
    }
    check(interval.startTime.seconds > 0) { "start_time required in data_availability_interval" }
    if (interval.hasEndTime()) {
      check(Timestamps.isValid(interval.endTime)) {
        "data_availability_interval.end_time must be a valid timestamp"
      }
      check(Timestamps.compare(interval.startTime, interval.endTime) < 0) {
        "data_availability_interval start_time must be before end_time"
      }
    }

    check(eventGroup.hasEventGroupMetadata()) { "Event Group Metadata must be set" }
    check(eventGroup.eventGroupMetadata.hasAdMetadata()) {
      "event_group_metadata.ad_metadata must be set"
    }
    check(eventGroup.eventGroupMetadata.adMetadata.hasCampaignMetadata()) {
      "event_group_metadata.ad_metadata.campaign_metadata must be set"
    }
    check(eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.brand.isNotBlank()) {
      "event_group_metadata.ad_metadata.campaign_metadata.brand must be non-blank"
    }
    check(eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.campaign.isNotBlank()) {
      "event_group_metadata.ad_metadata.campaign_metadata.campaign must be non-blank"
    }

    check(
      eventGroup.eventGroupReferenceId.isNotBlank() ||
        (eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank())
    ) {
      "Either Event Group Reference Id or Entity Key with entity ID must be set"
    }
    if (eventGroup.hasEntityKey()) {
      validateEntityKey(eventGroup.entityKey)
    }

    if (eventGroup.measurementConsumer.isNotBlank()) {
      validateMeasurementConsumer(eventGroup.measurementConsumer)
    } else {
      check(eventGroup.clientAccountReferenceId.isNotEmpty()) {
        "Either Measurement Consumer or Client Account Reference ID must be set"
      }
      check(eventGroup.clientAccountReferenceId.length <= MAX_CLIENT_ACCOUNT_REFERENCE_ID_LENGTH) {
        "client_account_reference_id must not exceed $MAX_CLIENT_ACCOUNT_REFERENCE_ID_LENGTH characters"
      }
    }
  }

  private fun validateDeleted(eventGroup: EventGroup) {
    check(eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank()) {
      "Entity Key with entity ID must be set for deleted Event Groups"
    }
    validateEntityKey(eventGroup.entityKey)
    check(eventGroup.measurementConsumer.isNotBlank()) {
      "Measurement Consumer must be set for deleted Event Groups"
    }
    validateMeasurementConsumer(eventGroup.measurementConsumer)
  }

  private fun validateEntityKey(entityKey: EventGroup.EntityKey) {
    check(entityKey.entityType.isNotBlank()) {
      "EventGroup.entity_key.entity_type must be non-blank"
    }
    check(entityKey.entityId.isNotBlank()) { "EventGroup.entity_key.entity_id must be non-blank" }
    check(URL_SAFE_VALUE.matches(entityKey.entityType)) {
      "EventGroup.entity_key.entity_type must be URL-safe"
    }
    check(URL_SAFE_VALUE.matches(entityKey.entityId)) {
      "EventGroup.entity_key.entity_id must be URL-safe"
    }
  }

  private fun validateMeasurementConsumer(measurementConsumer: String) {
    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumer)
    check(
      measurementConsumerKey != null && measurementConsumerKey.measurementConsumerId.isNotBlank()
    ) {
      "Malformed measurement_consumer: $measurementConsumer; expected " +
        "measurementConsumers/<measurement-consumer-id>"
    }
  }

  private const val MAX_CLIENT_ACCOUNT_REFERENCE_ID_LENGTH = 36
  private val URL_SAFE_VALUE = Regex("[A-Za-z0-9._~-]+")
}
