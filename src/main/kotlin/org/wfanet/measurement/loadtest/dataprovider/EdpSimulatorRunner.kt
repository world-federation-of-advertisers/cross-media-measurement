/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.type.Interval
import io.grpc.ManagedChannel
import java.io.File
import java.security.cert.X509Certificate
import java.time.LocalDate
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.loadtest.config.PrivacyBudgets
import picocli.CommandLine

class EdpSimulatorRunner : AbstractEdpSimulatorRunner<EdpSimulator>() {
  private class EventGroupOptions {
    @CommandLine.Option(
      names = ["--event-group-reference-id-suffix"],
      description = ["Suffix of the EventGroup reference ID"],
      required = true,
    )
    lateinit var referenceIdSuffix: String
      private set

    lateinit var syntheticDataSpec: SyntheticEventGroupSpec

    @CommandLine.Option(
      names = ["--event-group-synthetic-spec"],
      description = ["Path to SyntheticEventGroupSpec protobuf message in text format"],
      required = true,
    )
    private fun setSyntheticDataSpec(syntheticDataSpecFile: File) {
      syntheticDataSpec =
        parseTextProto(syntheticDataSpecFile, SyntheticEventGroupSpec.getDefaultInstance())
    }

    @CommandLine.Option(
      names = ["--event-group-media-type"],
      description = ["MediaType for the EventGroup. May be specified multiple times."],
      required = true,
    )
    lateinit var mediaTypes: Set<MediaType>
      private set

    class BrandAdCampaignMetadata {
      @CommandLine.Option(names = ["--event-group-brand-name"], required = true)
      lateinit var brandName: String
        private set

      @CommandLine.Option(names = ["--event-group-campaign-name"], required = true)
      lateinit var campaignName: String
        private set
    }

    @CommandLine.ArgGroup(exclusive = false)
    var brandAdCampaignMetadata: BrandAdCampaignMetadata? = null
      private set
  }

  @CommandLine.ArgGroup(
    multiplicity = "1..*",
    exclusive = false,
    heading = "EventGroup Configuration. May be specified multiple times.\n",
  )
  private lateinit var eventGroupsOptions: List<EventGroupOptions>

  private val eventGroupOptionsByReferenceIdSuffix: Map<String, EventGroupOptions> by lazy {
    eventGroupsOptions.associateBy { it.referenceIdSuffix }
  }

  override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
    val referenceIdSuffix =
      EdpSimulator.getEventGroupReferenceIdSuffix(eventGroup, edpData.displayName)
    val options: EventGroupOptions =
      eventGroupOptionsByReferenceIdSuffix.getValue(referenceIdSuffix)
    return options.syntheticDataSpec
  }

  override fun buildEdpSimulator(
    measurementConsumerName: String,
    kingdomPublicApiChannel: ManagedChannel,
    requisitionFulfillmentStubsByDuchyId:
      Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
    trustedCertificates: Map<ByteString, X509Certificate>,
    eventQuery: SyntheticGeneratorEventQuery,
    hmssVidIndexMap: InMemoryVidIndexMap?,
    logSketchDetails: Boolean,
    throttler: MinimumIntervalThrottler,
    health: SettableHealth,
    random: Random,
  ): EdpSimulator {
    return EdpSimulator(
      edpData,
      measurementConsumerName,
      CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel),
      DataProvidersGrpcKt.DataProvidersCoroutineStub(kingdomPublicApiChannel),
      EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdomPublicApiChannel),
      RequisitionsGrpcKt.RequisitionsCoroutineStub(kingdomPublicApiChannel),
      requisitionFulfillmentStubsByDuchyId,
      eventQuery,
      throttler,
      PrivacyBudgets.createNoOpPrivacyBudgetManager(),
      trustedCertificates,
      hmssVidIndexMap,
      random = random,
      logSketchDetails = logSketchDetails,
      health = health,
    )
  }

  override suspend fun EdpSimulator.ensureEventGroups() {
    for (eventGroupOptions in eventGroupOptionsByReferenceIdSuffix.values) {
      val brandAdCampaignMetadata: EventGroupOptions.BrandAdCampaignMetadata? =
        eventGroupOptions.brandAdCampaignMetadata
      val metadata: EventGroupMetadata? =
        if (brandAdCampaignMetadata == null) {
          null
        } else {
          eventGroupMetadata {
            adMetadata =
              EventGroupMetadataKt.adMetadata {
                campaignMetadata =
                  EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                    brandName = brandAdCampaignMetadata.brandName
                    campaignName = brandAdCampaignMetadata.campaignName
                  }
              }
          }
        }

      ensureEventGroup(
        eventGroupOptions.referenceIdSuffix,
        eventGroupOptions.mediaTypes,
        computeDataAvailabilityInterval(eventGroupOptions.syntheticDataSpec),
        metadata,
      )
    }
  }

  private fun computeDataAvailabilityInterval(
    syntheticEventGroupSpec: SyntheticEventGroupSpec
  ): Interval {
    var startDate = LocalDate.MAX
    var endDateExclusive = LocalDate.MIN
    for (dateSpec in syntheticEventGroupSpec.dateSpecsList) {
      startDate = minOf(startDate, dateSpec.dateRange.start.toLocalDate())
      endDateExclusive = maxOf(endDateExclusive, dateSpec.dateRange.endExclusive.toLocalDate())
    }

    val timeRange =
      OpenEndTimeRange(
        startDate.atStartOfDay(syntheticDataTimeZone).toInstant(),
        endDateExclusive.atStartOfDay(syntheticDataTimeZone).toInstant(),
      )
    return timeRange.toInterval()
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(EdpSimulatorRunner(), args)
  }
}
