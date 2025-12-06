// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.duration
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.math.log2
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.elGamalPublicKey as anySketchElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt.variance
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.DeterministicCount
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.Health
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.verifyElGamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.dataprovider.InvalidRequisitionException
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.dataprovider.MeasurementResults.computeImpression
import org.wfanet.measurement.dataprovider.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getDirectAcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getMpcAcdpQuery
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.shareshuffle.FulfillRequisitionRequestBuilder as ShareshuffleRequisitionRequestBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeRequisitionRequestBuilder
import org.wfanet.measurement.loadtest.common.sampleVids
import org.wfanet.measurement.loadtest.config.TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX

/** A simulator handling EDP businesses. */
abstract class AbstractEdpSimulator(
  edpData: DataProviderData,
  edpDisplayName: String,
  protected val measurementConsumerName: String,
  certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub,
  private val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val requisitionFulfillmentStubsByDuchyId:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  protected val syntheticDataTimeZone: ZoneId,
  protected open val eventGroupsOptions: Collection<EventGroupOptions>,
  protected val eventQuery: EventQuery<Message>,
  throttler: Throttler,
  private val privacyBudgetManager: PrivacyBudgetManager,
  trustedCertificates: Map<ByteString, X509Certificate>,
  /**
   * EDP uses the vidIndexMap to fulfill the requisitions for the honest majority share shuffle
   * protocol.
   *
   * When the vidIndexMap is empty, the honest majority share shuffle protocol is not supported.
   */
  private val vidIndexMap: VidIndexMap?,
  private val sketchEncrypter: SketchEncrypter,
  private val random: Random,
  private val logSketchDetails: Boolean,
  private val health: SettableHealth,
  private val blockingCoroutineContext: @BlockingExecutor CoroutineContext,
  private val trusTeeEncryptionParams: TrusTeeRequisitionRequestBuilder.EncryptionParams?,
) :
  RequisitionFulfiller(edpData, certificatesStub, requisitionsStub, throttler, trustedCertificates),
  Health by health {

  interface EventGroupOptions {
    val referenceIdSuffix: String
    val syntheticDataSpec: SyntheticEventGroupSpec
  }

  fun EventGroupOptions.getDataAvailabilityInterval(timeZone: ZoneId): Interval {
    var startDate = LocalDate.MAX
    var endDateExclusive = LocalDate.MIN
    for (dateSpec in syntheticDataSpec.dateSpecsList) {
      startDate = minOf(startDate, dateSpec.dateRange.start.toLocalDate())
      endDateExclusive = maxOf(endDateExclusive, dateSpec.dateRange.endExclusive.toLocalDate())
    }

    val timeRange =
      OpenEndTimeRange(
        startDate.atStartOfDay(timeZone).toInstant(),
        endDateExclusive.atStartOfDay(timeZone).toInstant(),
      )
    return timeRange.toInterval()
  }

  protected val eventGroupReferenceIdPrefix = getEventGroupReferenceIdPrefix(edpDisplayName)

  protected val edpData: DataProviderData
    get() = dataProviderData

  //  private val eventQuery = SimulatorEventQuery(syntheticPopulationSpec, eventMessageDescriptor)

  private val supportedProtocols = buildSet {
    add(ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2)
    add(ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2)
    if (vidIndexMap != null) {
      add(ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE)
      add(ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE)
    }
  }

  private val measurementConsumerKey =
    checkNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))

  /** A sequence of operations done in the simulator. */
  override suspend fun run() {
    updateDataProvider()

    withContext(blockingCoroutineContext) { health.setHealthy(true) }
    throttler.loopOnReady { executeRequisitionFulfillingWorkflow() }
  }

  private suspend fun updateDataProvider() {
    replaceDataAvailabilityIntervals()

    dataProvidersStub.replaceDataProviderCapabilities(
      replaceDataProviderCapabilitiesRequest {
        name = edpData.name
        capabilities =
          DataProviderKt.capabilities {
            honestMajorityShareShuffleSupported = (vidIndexMap != null)
          }
      }
    )
  }

  private suspend fun replaceDataAvailabilityIntervals() {
    val dataAvailabilityInterval = computeDataAvailabilityInterval()
    @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
    val modelLines: Flow<ModelLine> =
      modelLinesStub
        .listResources { pageToken: String ->
          val response: ListModelLinesResponse =
            listModelLines(
              listModelLinesRequest {
                parent = "modelProviders/-/modelSuites/-"
                filter =
                  ListModelLinesRequestKt.filter {
                    activeIntervalContains = dataAvailabilityInterval
                  }
                this.pageToken = pageToken
              }
            )
          ResourceList(response.modelLinesList, response.nextPageToken)
        }
        .flattenConcat()

    dataProvidersStub.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        name = edpData.name
        modelLines.collect { modelLine ->
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLine.name
              value = dataAvailabilityInterval
            }
        }
      }
    )
  }

  private fun computeDataAvailabilityInterval(): Interval {
    check(eventGroupsOptions.isNotEmpty())
    var start = Timestamps.MAX_VALUE
    var endExclusive = Timestamps.MIN_VALUE

    for (eventGroupOptions in eventGroupsOptions) {
      val interval = eventGroupOptions.getDataAvailabilityInterval(syntheticDataTimeZone)
      start = minOf(start, interval.startTime, Timestamps.comparator())
      endExclusive = maxOf(endExclusive, interval.endTime, Timestamps.comparator())
    }

    return interval {
      startTime = start
      endTime = endExclusive
    }
  }

  /** Ensures that appropriate [EventGroup]s exist for [eventGroupsOptions]. */
  abstract suspend fun ensureEventGroups(): List<EventGroup>

  protected suspend fun ensureEventGroup(
    referenceId: String,
    fillRequest: EventGroupKt.Dsl.() -> Unit,
  ): EventGroup {
    val existingEventGroup: EventGroup? = getEventGroupByReferenceId(referenceId)
    if (existingEventGroup == null) {
      val request = createEventGroupRequest {
        parent = edpData.name
        eventGroup = eventGroup {
          measurementConsumer = measurementConsumerName
          eventGroupReferenceId = referenceId
          fillRequest()
        }
      }

      return try {
        eventGroupsStub.createEventGroup(request).also {
          logger.info { "Successfully created ${it.name}..." }
        }
      } catch (e: StatusException) {
        throw Exception("Error creating event group", e)
      }
    }

    val request = updateEventGroupRequest { eventGroup = existingEventGroup.copy { fillRequest() } }
    return try {
      eventGroupsStub.updateEventGroup(request).also {
        logger.info { "Successfully updated ${it.name}..." }
      }
    } catch (e: StatusException) {
      throw Exception("Error updating event group", e)
    }
  }

  /**
   * Returns the first [EventGroup] for this `DataProvider` and [MeasurementConsumer] with
   * [eventGroupReferenceId], or `null` if not found.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  protected suspend fun getEventGroupByReferenceId(eventGroupReferenceId: String): EventGroup? {
    return eventGroupsStub
      .listResources { pageToken: String ->
        val response =
          try {
            listEventGroups(
              listEventGroupsRequest {
                parent = edpData.name
                filter =
                  ListEventGroupsRequestKt.filter {
                    measurementConsumerIn += measurementConsumerName
                  }
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
      .firstOrNull { it.eventGroupReferenceId == eventGroupReferenceId }
  }

  private fun verifyProtocolConfig(
    requsitionName: String,
    protocol: ProtocolConfig.Protocol.ProtocolCase,
  ) {
    if (protocol !in supportedProtocols) {
      logger.log(Level.WARNING, "Skipping $requsitionName: Protocol not supported.")
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Protocol not set or not supported.",
      )
    }
  }

  private fun verifyDuchyEntry(
    duchyEntry: DuchyEntry,
    duchyCertificate: Certificate,
    protocol: ProtocolConfig.Protocol.ProtocolCase,
  ) {
    val duchyX509Certificate: X509Certificate = readCertificate(duchyCertificate.x509Der)
    // Look up the trusted issuer certificate for this Duchy certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right Duchy. In a production environment,
    // consider having a mapping of Duchy to issuer certificate.
    val trustedIssuer =
      trustedCertificates[checkNotNull(duchyX509Certificate.authorityKeyIdentifier)]
        ?: throw InvalidConsentSignalException("Issuer of ${duchyCertificate.name} is not trusted")

    try {
      when (protocol) {
        ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2 ->
          verifyElGamalPublicKey(
            duchyEntry.value.liquidLegionsV2.elGamalPublicKey,
            duchyX509Certificate,
            trustedIssuer,
          )
        ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
          verifyElGamalPublicKey(
            duchyEntry.value.reachOnlyLiquidLegionsV2.elGamalPublicKey,
            duchyX509Certificate,
            trustedIssuer,
          )
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
          if (duchyEntry.value.honestMajorityShareShuffle.hasPublicKey()) {
            verifyEncryptionPublicKey(
              duchyEntry.value.honestMajorityShareShuffle.publicKey,
              duchyX509Certificate,
              trustedIssuer,
            )
          }
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE -> {}
        else -> throw InvalidRequisitionException("Unsupported protocol $protocol")
      }
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${duchyCertificate.name} is invalid",
        e,
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException(
        "ElGamal public key signature is invalid for Duchy ${duchyEntry.key}",
        e,
      )
    }
  }

  /**
   * Verify duchy entries.
   *
   * For each duchy entry, verifies its certificate. If the protocol is honest majority share
   * shuffle, also verify that there are exactly two duchy entires, and only one of them has the
   * encryption public key. Throws a RequisitionRefusalException if the verification fails.
   */
  private suspend fun verifyDuchyEntries(
    requisition: Requisition,
    protocol: ProtocolConfig.Protocol.ProtocolCase,
  ) {
    try {
      for (duchyEntry in requisition.duchiesList) {
        val duchyCertificate: Certificate = getCertificate(duchyEntry.value.duchyCertificate)
        verifyDuchyEntry(duchyEntry, duchyCertificate, protocol)
      }
    } catch (e: InvalidConsentSignalException) {
      logger.log(Level.WARNING, e) {
        "Consent signaling verification failed for ${requisition.name}"
      }
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
        e.message.orEmpty(),
      )
    }

    if (protocol == ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE) {
      if (requisition.duchiesList.size != 2) {
        val message =
          "Two duchy entries are expected, but there are ${requisition.duchiesList.size}."
        logger.log(Level.WARNING, message)
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          message,
        )
      }

      val publicKeyList =
        requisition.duchiesList
          .filter { it.value.honestMajorityShareShuffle.hasPublicKey() }
          .map { it.value.honestMajorityShareShuffle.publicKey }

      if (publicKeyList.size != 1) {
        val message =
          "Exactly one duchy entry is expected to have the encryption public key, but ${publicKeyList.size} duchy entries do."
        logger.log(Level.WARNING, message)
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          message,
        )
      }
    }

    if (protocol == ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE) {
      if (requisition.duchiesList.size != 1) {
        val message = "One duchy entry is expected, but there are ${requisition.duchiesList.size}."
        logger.log(Level.WARNING, message)
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          message,
        )
      }
    }
  }

  /** Executes the requisition fulfillment workflow. */
  override suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisitions =
      getRequisitions().filter {
        checkNotNull(MeasurementKey.fromName(it.measurement)).measurementConsumerId ==
          measurementConsumerKey.measurementConsumerId
      }

    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      try {
        logger.info("Processing requisition ${requisition.name}...")

        // TODO(@SanjayVas): Verify that DataProvider public key in Requisition matches private key
        // in edpData. A real EDP would look up the matching private key.

        val measurementConsumerCertificate: Certificate =
          getCertificate(requisition.measurementConsumerCertificate)

        val (measurementSpec, requisitionSpec) =
          try {
            verifySpecifications(requisition, measurementConsumerCertificate)
          } catch (e: InvalidConsentSignalException) {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              e.message.orEmpty(),
              e,
            )
          }

        logger.log(Level.INFO, "MeasurementSpec:\n$measurementSpec")
        logger.log(Level.INFO, "RequisitionSpec:\n$requisitionSpec")

        for (eventGroupEntry in requisitionSpec.events.eventGroupsList) {
          val eventGroupKey =
            EventGroupKey.fromName(eventGroupEntry.key)
              ?: throw RequisitionRefusalException.Test(
                Requisition.Refusal.Justification.SPEC_INVALID,
                "Invalid EventGroup resource name ${eventGroupEntry.key}",
              )
          val eventGroupId = eventGroupKey.eventGroupId
          if (eventGroupId == CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID) {
            throw RequisitionRefusalException.Test(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              "consent signal invalid",
            )
          }

          if (eventGroupId == SPEC_INVALID_EVENT_GROUP_ID) {
            throw RequisitionRefusalException.Test(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "spec invalid",
            )
          }

          if (eventGroupId == INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID) {
            throw RequisitionRefusalException.Test(
              Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
              "insufficient privacy budget",
            )
          }

          if (eventGroupId == UNFULFILLABLE_EVENT_GROUP_ID) {
            throw RequisitionRefusalException.Test(
              Requisition.Refusal.Justification.UNFULFILLABLE,
              "unfulfillable",
            )
          }

          if (eventGroupId == DECLINED_EVENT_GROUP_ID) {
            throw RequisitionRefusalException.Test(
              Requisition.Refusal.Justification.DECLINED,
              "declined",
            )
          }
        }

        val eventGroupSpecs: List<EventQuery.EventGroupSpec> = buildEventGroupSpecs(requisitionSpec)

        val requisitionFingerprint = computeRequisitionFingerprint(requisition)

        val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

        if (protocols.any { it.hasDirect() }) {
          val directProtocolConfig =
            requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
          val directNoiseMechanismOptions =
            directProtocolConfig.noiseMechanismsList
              .mapNotNull { protocolConfigNoiseMechanism ->
                protocolConfigNoiseMechanism.toDirectNoiseMechanism()
              }
              .toSet()

          if (measurementSpec.hasReach() || measurementSpec.hasReachAndFrequency()) {
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectReachAndFrequencyNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillDirectReachAndFrequencyMeasurement(
              requisition,
              measurementSpec,
              requisitionSpec.nonce,
              eventGroupSpecs,
              directProtocol,
            )
          } else if (measurementSpec.hasDuration()) {
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectImpressionNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillDurationMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs,
              directProtocol,
            )
          } else if (measurementSpec.hasImpression()) {
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectWatchDurationNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillImpressionMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs,
              directProtocol,
            )
          } else {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for direct fulfillment.",
            )
          }
        } else if (protocols.any { it.hasLiquidLegionsV2() }) {
          if (!measurementSpec.hasReach() && !measurementSpec.hasReachAndFrequency()) {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol llv2.",
            )
          }

          val protocolConfig = ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2
          verifyProtocolConfig(requisition.name, protocolConfig)
          verifyDuchyEntries(requisition, protocolConfig)

          fulfillRequisitionForLiquidLegionsV2Measurement(
            requisition,
            measurementSpec,
            requisitionFingerprint,
            requisitionSpec.nonce,
            eventGroupSpecs,
          )
        } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
          if (!measurementSpec.hasReach()) {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol rollv2.",
            )
          }

          val protocolConfig = ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2
          verifyProtocolConfig(requisition.name, protocolConfig)
          verifyDuchyEntries(requisition, protocolConfig)

          fulfillRequisitionForReachOnlyLiquidLegionsV2Measurement(
            requisition,
            measurementSpec,
            requisitionFingerprint,
            requisitionSpec.nonce,
            eventGroupSpecs,
          )
        } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
          if (!measurementSpec.hasReach() && !measurementSpec.hasReachAndFrequency()) {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol hmss.",
            )
          }

          val protocolConfig = ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE
          verifyProtocolConfig(requisition.name, protocolConfig)
          verifyDuchyEntries(requisition, protocolConfig)

          fulfillRequisitionForHmssMeasurement(
            requisition,
            measurementSpec,
            requisitionSpec.nonce,
            eventGroupSpecs,
          )
        } else if (protocols.any { it.hasTrusTee() }) {
          if (!measurementSpec.hasReach() && !measurementSpec.hasReachAndFrequency()) {
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol TrusTee.",
            )
          }

          val protocolConfig = ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE
          verifyProtocolConfig(requisition.name, protocolConfig)
          verifyDuchyEntries(requisition, protocolConfig)

          fulfillRequisitionForTrusTeeMeasurement(
            requisition,
            measurementSpec,
            requisitionSpec.nonce,
            eventGroupSpecs,
          )
        } else {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Protocol not set or not supported.",
          )
        }
      } catch (e: RequisitionRefusalException) {
        if (e !is RequisitionRefusalException.Test) {
          logger.log(Level.WARNING, e) { "Refusing Requisition ${requisition.name}" }
        }

        refuseRequisition(requisition.name, e.justification, e.message!!, requisition.etag)
      }
    }
  }

  private data class DirectProtocol(
    val directProtocolConfig: ProtocolConfig.Direct,
    val selectedDirectNoiseMechanism: DirectNoiseMechanism,
  )

  /**
   * Builds [EventQuery.EventGroupSpec]s from a [requisitionSpec] by fetching [EventGroup]s.
   *
   * @throws InvalidRequisitionException if [requisitionSpec] is found to be invalid
   */
  private suspend fun buildEventGroupSpecs(
    requisitionSpec: RequisitionSpec
  ): List<EventQuery.EventGroupSpec> {
    // TODO(@SanjayVas): Cache EventGroups.
    return requisitionSpec.events.eventGroupsList.map {
      val eventGroup =
        try {
          eventGroupsStub.getEventGroup(getEventGroupRequest { name = it.key })
        } catch (e: StatusException) {
          throw when (e.status.code) {
            Status.Code.NOT_FOUND -> InvalidRequisitionException("EventGroup $it not found", e)
            else -> Exception("Error retrieving EventGroup $it", e)
          }
        }

      if (!eventGroup.eventGroupReferenceId.startsWith(SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX)) {
        throw InvalidRequisitionException("EventGroup ${it.key} not supported by this simulator")
      }

      EventQuery.EventGroupSpec(eventGroup, it.value)
    }
  }

  private suspend fun chargeIndirectPrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    noiseMechanism: NoiseMechanism,
    contributorCount: Int,
  ) {
    logger.info(
      "chargeIndirectPrivacyBudget for requisition with $noiseMechanism noise mechanism..."
    )

    try {
      if (noiseMechanism != NoiseMechanism.DISCRETE_GAUSSIAN) {
        throw PrivacyBudgetManagerException(
          PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM
        )
      }

      privacyBudgetManager.chargePrivacyBudgetInAcdp(
        getMpcAcdpQuery(
          Reference(measurementConsumerName, requisitionName, false),
          measurementSpec,
          eventSpecs,
          contributorCount,
        )
      )
    } catch (e: PrivacyBudgetManagerException) {
      logger.log(Level.WARNING, "chargeIndirectPrivacyBudget failed due to ${e.errorType}", e)
      when (e.errorType) {
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
            "Privacy budget exceeded",
          )
        }
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Invalid event filter",
          )
        }
        PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Incorrect noise mechanism. Should be DISCRETE_GAUSSIAN for ACDP composition but is $noiseMechanism",
          )
        }
        PrivacyBudgetManagerExceptionType.DATABASE_UPDATE_ERROR,
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED -> {
          throw Exception("Unexpected PBM error", e)
        }
      }
    }
  }

  private suspend fun chargeDirectPrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    directNoiseMechanism: DirectNoiseMechanism,
  ) {
    logger.info("chargeDirectPrivacyBudget for requisition $requisitionName...")

    try {
      if (directNoiseMechanism != DirectNoiseMechanism.CONTINUOUS_GAUSSIAN) {
        throw PrivacyBudgetManagerException(
          PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM
        )
      }

      privacyBudgetManager.chargePrivacyBudgetInAcdp(
        getDirectAcdpQuery(
          Reference(measurementConsumerName, requisitionName, false),
          measurementSpec,
          eventSpecs,
        )
      )
    } catch (e: PrivacyBudgetManagerException) {
      logger.log(Level.WARNING, "chargeDirectPrivacyBudget failed due to ${e.errorType}", e)
      when (e.errorType) {
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
            "Privacy budget exceeded",
          )
        }
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Invalid event filter",
          )
        }
        PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM -> {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Incorrect noise mechanism. Should be GAUSSIAN for ACDP composition but is $directNoiseMechanism",
          )
        }
        PrivacyBudgetManagerExceptionType.DATABASE_UPDATE_ERROR,
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED -> {
          throw Exception("Unexpected PBM error", e)
        }
      }
    }
  }

  private fun logSketchDetails(sketch: Sketch) {
    val sortedRegisters = sketch.registersList.sortedBy { it.index }
    for (register in sortedRegisters) {
      logger.log(Level.INFO) { "${register.index}, ${register.valuesList.joinToString()}" }
    }
  }

  private fun generateSketch(
    sketchConfig: SketchConfig,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ): Sketch {
    logger.info("Generating Sketch...")
    val sketch =
      SketchGenerator(eventQuery, sketchConfig, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    logger.log(Level.INFO) { "SketchConfig:\n${sketch.config}" }
    logger.log(Level.INFO) { "Registers Size:\n${sketch.registersList.size}" }
    if (logSketchDetails) {
      logSketchDetails(sketch)
    }

    return sketch
  }

  private fun encryptLiquidLegionsV2Sketch(
    sketch: Sketch,
    ellipticCurveId: Int,
    combinedPublicKey: AnySketchElGamalPublicKey,
    maximumValue: Int,
  ): ByteString {
    require(maximumValue > 0) { "Maximum value must be positive" }
    logger.log(Level.INFO, "Encrypting Liquid Legions V2 Sketch...")
    return sketchEncrypter.encrypt(sketch, ellipticCurveId, combinedPublicKey, maximumValue)
  }

  private fun encryptReachOnlyLiquidLegionsV2Sketch(
    sketch: Sketch,
    ellipticCurveId: Int,
    combinedPublicKey: AnySketchElGamalPublicKey,
  ): ByteString {
    logger.log(Level.INFO, "Encrypting Reach-Only Liquid Legions V2 Sketch...")
    return sketchEncrypter.encrypt(sketch, ellipticCurveId, combinedPublicKey)
  }

  /**
   * Fulfill Liquid Legions V2 Measurement's Requisition by creating an encrypted sketch and send to
   * the duchy.
   */
  private suspend fun fulfillRequisitionForLiquidLegionsV2Measurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ) {
    val llv2Protocol: ProtocolConfig.Protocol =
      requireNotNull(
        requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasLiquidLegionsV2() }
      ) {
        "Protocol with LiquidLegionsV2 is missing"
      }
    val liquidLegionsV2: ProtocolConfig.LiquidLegionsV2 = llv2Protocol.liquidLegionsV2
    val combinedPublicKey = requisition.getCombinedPublicKey(liquidLegionsV2.ellipticCurveId)

    chargeIndirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      liquidLegionsV2.noiseMechanism,
      requisition.duchiesCount,
    )

    val sketch =
      try {
        generateSketch(
          liquidLegionsV2.sketchParams.toSketchConfig(),
          measurementSpec,
          eventGroupSpecs,
        )
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e,
        )
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}",
        )
      }

    val encryptedSketch =
      encryptLiquidLegionsV2Sketch(
        sketch,
        liquidLegionsV2.ellipticCurveId,
        combinedPublicKey,
        measurementSpec.reachAndFrequency.maximumFrequency.coerceAtLeast(1),
      )
    fulfillRequisition(requisition, requisitionFingerprint, nonce, encryptedSketch)
  }

  /**
   * Fulfill Reach-Only Liquid Legions V2 Measurement's Requisition by creating an encrypted sketch
   * and send to the duchy.
   */
  private suspend fun fulfillRequisitionForReachOnlyLiquidLegionsV2Measurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ) {
    val protocolConfig: ProtocolConfig.ReachOnlyLiquidLegionsV2 =
      requireNotNull(
          requisition.protocolConfig.protocolsList.find { protocol ->
            protocol.hasReachOnlyLiquidLegionsV2()
          }
        ) {
          "Protocol with ReachOnlyLiquidLegionsV2 is missing"
        }
        .reachOnlyLiquidLegionsV2
    val combinedPublicKey: AnySketchElGamalPublicKey =
      requisition.getCombinedPublicKey(protocolConfig.ellipticCurveId)

    chargeIndirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      protocolConfig.noiseMechanism,
      requisition.duchiesCount,
    )

    val sketch =
      try {
        generateSketch(
          protocolConfig.sketchParams.toSketchConfig(),
          measurementSpec,
          eventGroupSpecs,
        )
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e,
        )
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}",
        )
      }

    val encryptedSketch =
      encryptReachOnlyLiquidLegionsV2Sketch(
        sketch,
        protocolConfig.ellipticCurveId,
        combinedPublicKey,
      )
    fulfillRequisition(requisition, requisitionFingerprint, nonce, encryptedSketch)
  }

  private suspend fun fulfillRequisition(
    requisition: Requisition,
    requisitionFingerprint: ByteString,
    nonce: Long,
    data: ByteString,
  ) {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val requests: Flow<FulfillRequisitionRequest> = flow {
      logger.info { "Emitting FulfillRequisitionRequests..." }
      emit(
        fulfillRequisitionRequest {
          header = header {
            name = requisition.name
            this.requisitionFingerprint = requisitionFingerprint
            this.nonce = nonce
            etag = requisition.etag
          }
        }
      )
      emitAll(
        data.asBufferedFlow(RPC_CHUNK_SIZE_BYTES).map {
          fulfillRequisitionRequest { bodyChunk = bodyChunk { this.data = it } }
        }
      )
    }
    fulfillRequisition(requisitionFulfillmentStubsByDuchyId.values.first(), requisition, requests)
  }

  private suspend fun fulfillRequisition(
    requisitionFulfillmentStub: RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub,
    requisition: Requisition,
    requests: Flow<FulfillRequisitionRequest>,
  ) {
    logger.info("Fulfilling requisition ${requisition.name}...")
    try {
      requisitionFulfillmentStub.fulfillRequisition(requests)
    } catch (e: StatusException) {
      throw Exception("Error fulfilling requisition ${requisition.name}", e)
    }
  }

  private fun getDuchyWithoutPublicKey(requisition: Requisition): String {
    return requisition.duchiesList
      .singleOrNull { !it.value.honestMajorityShareShuffle.hasPublicKey() }
      ?.key
      ?: throw IllegalArgumentException(
        "Expected exactly one Duchy entry with an HMSS encryption public key."
      )
  }

  /** Fulfill Honest Majority Share Shuffle Measurement's Requisition. */
  private suspend fun fulfillRequisitionForHmssMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ) {
    requireNotNull(vidIndexMap) { "HMSS VidIndexMap cannot be null." }

    val protocolConfig: ProtocolConfig.HonestMajorityShareShuffle =
      requireNotNull(
          requisition.protocolConfig.protocolsList.find { protocol ->
            protocol.hasHonestMajorityShareShuffle()
          }
        ) {
          "Protocol with HonestMajorityShareShuffle is missing"
        }
        .honestMajorityShareShuffle

    chargeIndirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      protocolConfig.noiseMechanism,
      requisition.duchiesCount - 1,
    )

    logger.info("Generating sampled frequency vector for HMSS...")
    val frequencyVectorBuilder =
      FrequencyVectorBuilder(
        vidIndexMap.populationSpec,
        measurementSpec,
        overrideImpressionMaxFrequencyPerUser = null,
        strict = false,
      )
    for (eventGroupSpec in eventGroupSpecs) {
      try {
        eventQuery.getUserVirtualIds(eventGroupSpec).forEach {
          frequencyVectorBuilder.increment(vidIndexMap[it])
        }
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e,
        )
        throw RequisitionRefusalException.Default(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}",
        )
      }
    }

    val sampledFrequencyVector = frequencyVectorBuilder.build()
    logger.log(Level.INFO) { "Sampled frequency vector size:\n${sampledFrequencyVector.dataCount}" }
    val etag =
      requisitionsStub.getRequisition(getRequisitionRequest { name = requisition.name }).etag
    val requests =
      ShareshuffleRequisitionRequestBuilder.build(
          requisition,
          nonce,
          sampledFrequencyVector,
          edpData.certificateKey,
          edpData.signingKeyHandle,
          etag,
        )
        .asFlow()

    val duchyId = getDuchyWithoutPublicKey(requisition)
    val requisitionFulfillmentStub =
      requisitionFulfillmentStubsByDuchyId[duchyId]
        ?: throw Exception("Requisition fulfillment stub not found for $duchyId.")
    fulfillRequisition(requisitionFulfillmentStub, requisition, requests)
  }

  /** Fulfill Trustee Measurement's Requisition. */
  private suspend fun fulfillRequisitionForTrusTeeMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ) {
    requireNotNull(vidIndexMap) { "TrusTee VidIndexMap cannot be null." }

    requireNotNull(
      requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasTrusTee() }
    ) {
      "Protocol with TrusTee is missing"
    }

    chargeIndirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      NoiseMechanism.DISCRETE_GAUSSIAN,
      1,
    )

    logger.info("Generating sampled frequency vector for TrusTee...")
    val frequencyVectorBuilder =
      FrequencyVectorBuilder(
        vidIndexMap.populationSpec,
        measurementSpec,
        overrideImpressionMaxFrequencyPerUser = null,
        strict = false,
      )
    for (eventGroupSpec in eventGroupSpecs) {
      eventQuery.getUserVirtualIds(eventGroupSpec).forEach {
        frequencyVectorBuilder.increment(vidIndexMap[it])
      }
    }

    val sampledFrequencyVector = frequencyVectorBuilder.build()
    logger.log(Level.INFO) { "Sampled frequency vector size:\n${sampledFrequencyVector.dataCount}" }

    val requests: Flow<FulfillRequisitionRequest> =
      if (trusTeeEncryptionParams != null) {
        TrusTeeRequisitionRequestBuilder.buildEncrypted(
            requisition,
            nonce,
            sampledFrequencyVector,
            trusTeeEncryptionParams,
          )
          .asFlow()
      } else {
        TrusTeeRequisitionRequestBuilder.buildUnencrypted(
            requisition,
            nonce,
            sampledFrequencyVector,
          )
          .asFlow()
      }

    val duchyId = requisition.duchiesList.single().key
    val requisitionFulfillmentStub =
      requisitionFulfillmentStubsByDuchyId[duchyId]
        ?: throw Exception("Requisition fulfillment stub not found for $duchyId.")
    fulfillRequisition(requisitionFulfillmentStub, requisition, requests)
  }

  private fun Requisition.getCombinedPublicKey(curveId: Int): AnySketchElGamalPublicKey {
    logger.info("Getting combined public key...")
    val elGamalPublicKeys: List<AnySketchElGamalPublicKey> =
      this.duchiesList.map {
        val value: DuchyEntry.Value = it.value
        val signedElGamalPublicKey: SignedMessage =
          when (value.protocolCase) {
            DuchyEntry.Value.ProtocolCase.LIQUID_LEGIONS_V2 ->
              value.liquidLegionsV2.elGamalPublicKey
            DuchyEntry.Value.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
              value.reachOnlyLiquidLegionsV2.elGamalPublicKey
            else -> throw Exception("Invalid protocol to get combined public key.")
          }
        signedElGamalPublicKey.unpack<ElGamalPublicKey>().toAnySketchElGamalPublicKey()
      }

    return SketchEncrypter.combineElGamalPublicKeys(curveId, elGamalPublicKeys)
  }

  /**
   * Calculate direct reach and frequency for measurement with single EDP by summing up VIDs
   * directly and fulfillDirectMeasurement
   */
  private suspend fun fulfillDirectReachAndFrequencyMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    directProtocol: DirectProtocol,
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      directProtocol.selectedDirectNoiseMechanism,
    )

    logger.info("Calculating direct reach and frequency...")
    val measurementResult =
      buildDirectMeasurementResult(
        directProtocol,
        measurementSpec,
        sampleVids(eventGroupSpecs, measurementSpec.vidSamplingInterval),
      )

    fulfillDirectMeasurement(requisition, measurementSpec, nonce, measurementResult)
  }

  /**
   * Samples VIDs from multiple [EventQuery.EventGroupSpec]s with a
   * [MeasurementSpec.VidSamplingInterval].
   */
  private fun sampleVids(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
  ): Iterable<Long> {
    return try {
      sampleVids(eventQuery, eventGroupSpecs, vidSamplingInterval.start, vidSamplingInterval.width)
    } catch (e: EventFilterValidationException) {
      logger.log(
        Level.WARNING,
        "RequisitionFulfillmentWorkflow failed due to invalid event filter",
        e,
      )
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Invalid event filter (${e.code}): ${e.code.description}",
      )
    }
  }

  private fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random,
  ): AbstractNoiser =
    when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE ->
        object : AbstractNoiser() {
          override val distribution = ConstantRealDistribution(0.0)
          override val variance: Double
            get() = distribution.numericalVariance
        }
      DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
        LaplaceNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
      DirectNoiseMechanism.CONTINUOUS_GAUSSIAN ->
        GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
    }

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param reachValue Direct reach value.
   * @param privacyParams Differential privacy params for reach.
   * @param directNoiseMechanism Selected noise mechanism for direct reach.
   * @return Noised non-negative reach value.
   */
  private fun addReachPublisherNoise(
    reachValue: Int,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Int {
    val reachNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return max(0, reachValue + reachNoiser.sample().toInt())
  }

  /**
   * Add publisher noise to calculated direct frequency.
   *
   * @param reachValue Direct reach value.
   * @param frequencyMap Direct frequency.
   * @param privacyParams Differential privacy params for frequency map.
   * @param directNoiseMechanism Selected noise mechanism for direct frequency.
   * @return Noised non-negative frequency map.
   */
  private fun addFrequencyPublisherNoise(
    reachValue: Int,
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Map<Int, Double> {
    val frequencyNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    // Add noise to the histogram and cap negative values to zeros.
    val frequencyHistogram: Map<Int, Int> =
      frequencyMap.mapValues { (_, percentage) ->
        // Round the noise for privacy.
        val noisedCount: Int =
          (percentage * reachValue).roundToInt() + (frequencyNoiser.sample()).roundToInt()
        max(0, noisedCount)
      }
    val normalizationTerm: Double = frequencyHistogram.values.sum().toDouble()
    // Normalize to get the distribution
    return if (normalizationTerm != 0.0) {
      frequencyHistogram.mapValues { (_, count) -> count / normalizationTerm }
    } else {
      frequencyHistogram.mapValues { 0.0 }
    }
  }

  /**
   * Add publisher noise to calculated impression.
   *
   * @param impressionValue Impression value.
   * @param impressionMeasurementSpec Measurement spec of impression.
   * @param directNoiseMechanism Selected noise mechanism for impression.
   * @return Noised non-negative impression value.
   */
  private fun addImpressionPublisherNoise(
    impressionValue: Long,
    impressionMeasurementSpec: MeasurementSpec.Impression,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Long {
    val noiser: AbstractNoiser =
      getPublisherNoiser(impressionMeasurementSpec.privacyParams, directNoiseMechanism, random)
    // Noise needs to be scaled by maximumFrequencyPerUser.
    val noise = noiser.sample() * impressionMeasurementSpec.maximumFrequencyPerUser
    return max(0L, impressionValue + noise.roundToInt())
  }

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param measurementSpec Measurement spec.
   * @param samples sampled events.
   * @return [Measurement.Result].
   */
  private fun buildDirectMeasurementResult(
    directProtocol: DirectProtocol,
    measurementSpec: MeasurementSpec,
    samples: Iterable<Long>,
  ): Measurement.Result {
    val directProtocolConfig = directProtocol.directProtocolConfig
    val directNoiseMechanism = directProtocol.selectedDirectNoiseMechanism
    val protocolConfigNoiseMechanism = directNoiseMechanism.toProtocolConfigNoiseMechanism()

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
        if (!directProtocolConfig.hasDeterministicCountDistinct()) {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct reach computation.",
          )
        }
        if (!directProtocolConfig.hasDeterministicDistribution()) {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct frequency distribution computation.",
          )
        }

        val (sampledReachValue, frequencyMap) =
          MeasurementResults.computeReachAndFrequency(
            samples,
            measurementSpec.reachAndFrequency.maximumFrequency,
          )

        logger.info("Adding $directNoiseMechanism publisher noise to direct reach and frequency...")
        val sampledNoisedReachValue =
          addReachPublisherNoise(
            sampledReachValue,
            measurementSpec.reachAndFrequency.reachPrivacyParams,
            directNoiseMechanism,
          )
        val noisedFrequencyMap =
          addFrequencyPublisherNoise(
            sampledReachValue,
            frequencyMap,
            measurementSpec.reachAndFrequency.frequencyPrivacyParams,
            directNoiseMechanism,
          )

        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          reach = reach {
            value = scaledNoisedReachValue
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
          }
          frequency = frequency {
            relativeFrequencyDistribution.putAll(noisedFrequencyMap.mapKeys { it.key.toLong() })
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicDistribution = DeterministicDistribution.getDefaultInstance()
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
        if (!directProtocolConfig.hasDeterministicCount()) {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for impression computation.",
          )
        }

        val sampledImpressionCount =
          computeImpression(samples, measurementSpec.impression.maximumFrequencyPerUser)

        logger.info("Adding $directNoiseMechanism publisher noise to impression...")
        val sampledNoisedImpressionCount =
          addImpressionPublisherNoise(
            sampledImpressionCount,
            measurementSpec.impression,
            directNoiseMechanism,
          )
        val scaledNoisedImpressionCount =
          (sampledNoisedImpressionCount / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          impression = impression {
            value = scaledNoisedImpressionCount
            noiseMechanism = protocolConfigNoiseMechanism
            deterministicCount = DeterministicCount.getDefaultInstance()
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        val externalDataProviderId =
          apiIdToExternalId(DataProviderKey.fromName(edpData.name)!!.dataProviderId)
        MeasurementKt.result {
          watchDuration = watchDuration {
            value = duration {
              // Use a value based on the externalDataProviderId since it's a known value the
              // MeasurementConsumerSimulator can verify.
              seconds = log2(externalDataProviderId.toDouble()).toLong()
            }
            noiseMechanism = protocolConfigNoiseMechanism
            customDirectMethodology = customDirectMethodology {
              variance = variance { scalar = 0.0 }
            }
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        error("Measurement type not supported.")
      }
      MeasurementSpec.MeasurementTypeCase.REACH -> {
        if (!directProtocolConfig.hasDeterministicCountDistinct()) {
          throw RequisitionRefusalException.Default(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct reach computation.",
          )
        }

        val sampledReachValue = MeasurementResults.computeReach(samples)

        logger.info("Adding $directNoiseMechanism publisher noise to direct reach for reach-only")
        val sampledNoisedReachValue =
          addReachPublisherNoise(
            sampledReachValue,
            measurementSpec.reach.privacyParams,
            directNoiseMechanism,
          )
        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          reach = reach {
            value = scaledNoisedReachValue
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
        error("Measurement type not set.")
      }
    }
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  private fun selectReachAndFrequencyNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for reach or frequency measurements.",
      )
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for impression measurements from the overlap
   * of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism] [options].
   */
  private fun selectImpressionNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for impression measurements.",
      )
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for watch duration measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  private fun selectWatchDurationNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for watch duration measurements.",
      )
  }

  private suspend fun fulfillImpressionMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    directProtocol: DirectProtocol,
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      directProtocol.selectedDirectNoiseMechanism,
    )

    logger.info("Calculating impression...")
    val measurementResult =
      buildDirectMeasurementResult(
        directProtocol,
        measurementSpec,
        sampleVids(eventGroupSpecs, measurementSpec.vidSamplingInterval),
      )

    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  private suspend fun fulfillDurationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    directProtocol: DirectProtocol,
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      directProtocol.selectedDirectNoiseMechanism,
    )

    val measurementResult =
      buildDirectMeasurementResult(directProtocol, measurementSpec, listOf<Long>().asIterable())

    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  companion object {
    init {
      System.loadLibrary("secret_share_generator_adapter")
    }

    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // The direct noise mechanisms for ACDP composition in PBM for direct measurements in order
    // of preference. Currently, ACDP composition only supports CONTINUOUS_GAUSSIAN noise for direct
    // measurements.
    private val DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES =
      listOf(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)

    // Resource ID for EventGroup that fails Requisitions with CONSENT_SIGNAL_INVALID if used.
    private const val CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID = "consent-signal-invalid"
    // Resource ID for EventGroup that fails Requisitions with SPEC_INVALID if used.
    private const val SPEC_INVALID_EVENT_GROUP_ID = "spec-invalid"
    // Resource ID for EventGroup that fails Requisitions with INSUFFICIENT_PRIVACY_BUDGET if used.
    private const val INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID = "insufficient-privacy-budget"
    // Resource ID for EventGroup that fails Requisitions with UNFULFILLABLE if used.
    private const val UNFULFILLABLE_EVENT_GROUP_ID = "unfulfillable"
    // Resource ID for EventGroup that fails Requisitions with DECLINED if used.
    private const val DECLINED_EVENT_GROUP_ID = "declined"

    fun getEventGroupReferenceIdPrefix(edpDisplayName: String): String {
      return "$SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX-${edpDisplayName}"
    }

    fun getEventGroupReferenceIdSuffix(eventGroup: EventGroup, edpDisplayName: String): String {
      val prefix = getEventGroupReferenceIdPrefix(edpDisplayName)
      return eventGroup.eventGroupReferenceId.removePrefix(prefix)
    }
  }
}

private fun DirectNoiseMechanism.toProtocolConfigNoiseMechanism(): NoiseMechanism {
  return when (this) {
    DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
    DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
  }
}

/**
 * Converts a [NoiseMechanism] to a nullable [DirectNoiseMechanism].
 *
 * @return [DirectNoiseMechanism] when there is a matched, otherwise null.
 */
private fun NoiseMechanism.toDirectNoiseMechanism(): DirectNoiseMechanism? {
  return when (this) {
    NoiseMechanism.NONE -> DirectNoiseMechanism.NONE
    NoiseMechanism.CONTINUOUS_LAPLACE -> DirectNoiseMechanism.CONTINUOUS_LAPLACE
    NoiseMechanism.CONTINUOUS_GAUSSIAN -> DirectNoiseMechanism.CONTINUOUS_GAUSSIAN
    NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    NoiseMechanism.GEOMETRIC,
    NoiseMechanism.DISCRETE_GAUSSIAN,
    NoiseMechanism.UNRECOGNIZED -> {
      null
    }
  }
}

private fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): AnySketchElGamalPublicKey {
  val source = this
  return anySketchElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}
