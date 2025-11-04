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
import io.grpc.ManagedChannel
import java.io.File
import java.security.cert.X509Certificate
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeFulfillRequisitionRequestBuilder
import org.wfanet.measurement.loadtest.config.PrivacyBudgets
import picocli.CommandLine

class EdpSimulatorRunner : AbstractEdpSimulatorRunner() {
  private class EventGroupOptions : EdpSimulator.EventGroupOptions {
    @CommandLine.Option(
      names = ["--event-group-reference-id-suffix"],
      description = ["Suffix of the EventGroup reference ID"],
      required = true,
    )
    override lateinit var referenceIdSuffix: String
      private set

    override lateinit var syntheticDataSpec: SyntheticEventGroupSpec

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
    override lateinit var mediaTypes: Set<MediaType>
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
    private var brandAdCampaignMetadata: BrandAdCampaignMetadata? = null

    override val metadata: EventGroupMetadata? by lazy {
      val brandAdCampaignMetadata = brandAdCampaignMetadata
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
    }
  }

  @CommandLine.ArgGroup(
    multiplicity = "1..*",
    exclusive = false,
    heading = "EventGroup Configuration. May be specified multiple times.\n",
  )
  private lateinit var _eventGroupsOptions: List<EventGroupOptions>

  override val eventGroupsOptions: List<EdpSimulator.EventGroupOptions>
    get() = _eventGroupsOptions

  override fun buildEdpSimulator(
    edpDisplayName: String,
    measurementConsumerName: String,
    kingdomPublicApiChannel: ManagedChannel,
    requisitionFulfillmentStubsByDuchyId:
      Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
    trustedCertificates: Map<ByteString, X509Certificate>,
    eventQuery: SyntheticGeneratorEventQuery,
    vidIndexMap: InMemoryVidIndexMap?,
    logSketchDetails: Boolean,
    throttler: MinimumIntervalThrottler,
    health: SettableHealth,
    random: Random,
    trusTeeEncryptionParams: TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams?,
  ): AbstractEdpSimulator {
    return EdpSimulator(
      edpData,
      edpDisplayName,
      measurementConsumerName,
      CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel),
      ModelLinesGrpcKt.ModelLinesCoroutineStub(kingdomPublicApiChannel),
      DataProvidersGrpcKt.DataProvidersCoroutineStub(kingdomPublicApiChannel),
      EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdomPublicApiChannel),
      RequisitionsGrpcKt.RequisitionsCoroutineStub(kingdomPublicApiChannel),
      requisitionFulfillmentStubsByDuchyId,
      syntheticDataTimeZone,
      eventGroupsOptions,
      eventQuery,
      throttler,
      PrivacyBudgets.createNoOpPrivacyBudgetManager(),
      trustedCertificates,
      vidIndexMap,
      random = random,
      logSketchDetails = logSketchDetails,
      health = health,
      trusTeeEncryptionParams = trusTeeEncryptionParams,
    )
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(EdpSimulatorRunner(), args)
  }
}
