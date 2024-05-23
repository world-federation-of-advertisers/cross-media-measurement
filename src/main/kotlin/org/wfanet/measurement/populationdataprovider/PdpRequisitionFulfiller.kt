// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.populationdataprovider

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import io.grpc.Status
import io.grpc.StatusException
import java.security.cert.X509Certificate
import java.util.logging.Level
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec.VidRange
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters


data class PopulationInfo (
  val populationSpec: PopulationSpec,
  val eventDescriptor: Descriptor,
  val typeRegistry: TypeRegistry
)
/** A requisition fulfiller for PDP businesses. */
class PdpRequisitionFulfiller(
  pdpData: DataProviderData,
  certificatesStub: CertificatesCoroutineStub,
  requisitionsStub: RequisitionsCoroutineStub,
  throttler: Throttler,
  trustedCertificates: Map<ByteString, X509Certificate>,
  measurementConsumerName: String,
  private val modelRolloutsStub: ModelRolloutsCoroutineStub,
  private val modelReleasesStub: ModelReleasesCoroutineStub,
  private val populationSpecMap: Map<PopulationKey, PopulationInfo>,
) :
  DataProviderRequisitionFulfiller(
    pdpData,
    certificatesStub,
    requisitionsStub,
    throttler,
    trustedCertificates,
    measurementConsumerName
  ) {

  /** A sequence of operations done in the simulator. */
  override suspend fun run() {
    throttler.loopOnReady { executeRequisitionFulfillingWorkflow() }
  }
  /** Executes the requisition fulfillment workflow. */
  override suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisitions =
      getRequisitions().filter {
        checkNotNull(MeasurementKey.fromName(it.measurement)).measurementConsumerId ==
          checkNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
            .measurementConsumerId
      }

    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      try {
        logger.info("Processing requisition ${requisition.name}...")

        // TODO(@SanjayVas): Verify that DataProvider public key in Requisition matches private key
        // in pdpData. A real PDP would look up the matching private key.

        val measurementConsumerCertificate: Certificate =
          getCertificate(requisition.measurementConsumerCertificate)

        val (measurementSpec, requisitionSpec) =
          try {
            verifySpecifications(requisition, measurementConsumerCertificate)
          } catch (e: InvalidConsentSignalException) {
            logger.log(Level.WARNING, e) {
              "Consent signaling verification failed for ${requisition.name}"
            }
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              e.message.orEmpty(),
            )
          }

        logger.log(Level.INFO, "MeasurementSpec:\n$measurementSpec")
        logger.log(Level.INFO, "RequisitionSpec:\n$requisitionSpec")

        val modelRelease = getModelRelease(measurementSpec)

        val populationId = PopulationKey.fromName(modelRelease.population)
        if(populationId === null){
          throw InvalidSpecException("Measurement spec model line does not contain a valid Population for the model release of its latest model rollout.")
        }

        val populationInfo = populationSpecMap.getValue(populationId)
        val populationSpecValidator = PopulationSpecValidator.validateVidRangesList(populationInfo.populationSpec)
        if(populationSpecValidator.isFailure){
          throw InvalidSpecException("Population Spec $populationId is Invalid.")
        }


        val requisitionFilterExpression = requisitionSpec.population.filter.expression

        fulfillPopulationMeasurement(
          requisition,
          requisitionSpec,
          measurementSpec,
          requisitionFilterExpression,
          populationInfo
        )
      } catch (refusalException: RequisitionRefusalException) {
        refuseRequisition(
          requisition.name,
          refusalException.justification,
          refusalException.message ?: "Refuse to fulfill requisition.",
        )
      }
    }
  }

  /**
   * Returns the [ModelRelease] associated with the latest `ModelRollout` that is connected to the
   * `ModelLine` provided in the MeasurementSpec`
   */
  private suspend fun getModelRelease(measurementSpec: MeasurementSpec): ModelRelease {
    val measurementSpecModelLineName = measurementSpec.modelLine

    // Returns list of ModelRollouts.
    val listModelRolloutsResponse =
      try {
        modelRolloutsStub.listModelRollouts(
          listModelRolloutsRequest { parent = measurementSpecModelLineName }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            InvalidSpecException("ModelLine $measurementSpecModelLineName not found", e)
          else -> Exception("Error retrieving ModelLine $measurementSpecModelLineName", e)
        }
      }

    // Sort list of ModelRollouts by descending updateTime.
    val sortedModelRolloutsList =
      listModelRolloutsResponse.modelRolloutsList.sortedWith { a, b ->
        val aDate =
          if (a.hasGradualRolloutPeriod()) a.gradualRolloutPeriod.endDate else a.instantRolloutDate
        val bDate =
          if (b.hasGradualRolloutPeriod()) b.gradualRolloutPeriod.endDate else b.instantRolloutDate
        if (aDate.toLocalDate().isBefore(bDate.toLocalDate())) -1 else 1
      }

    // Retrieves latest ModelRollout from list.
    val latestModelRollout = sortedModelRolloutsList.first()
    val modelReleaseName = latestModelRollout.modelRelease

    // Returns ModelRelease associated with latest ModelRollout.
    return try {
      modelReleasesStub.getModelRelease(getModelReleaseRequest { name = modelReleaseName })
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> InvalidSpecException("ModelRelease $modelReleaseName not found", e)
        else -> Exception("Error retrieving ModelLine $modelReleaseName", e)
      }
    }
  }


  private suspend fun fulfillPopulationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    filterExpression: String,
    populationInfo: PopulationInfo
  ) {
    // Filters populationBucketsList through a CEL program and sums the result.
    val populationSum = getTotalPopulation(populationInfo, filterExpression)

    // Create measurement result with sum of valid populations.
    val measurementResult =
      MeasurementKt.result {
        population = MeasurementKt.ResultKt.population { value = populationSum }
      }

    // Fulfill the measurement.
    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  /** Returns the total sum of populations that reflect the filter expression. */
  private fun getTotalPopulation(
    populationInfo: PopulationInfo,
    filterExpression: String,
  ): Long {
    val subPopulationList = populationInfo.populationSpec.subpopulationsList
    return subPopulationList.sumOf {
      val attributesList = it.attributesList
      val vidRanges = it.vidRangesList
      val shouldSumPopulation = isValidAttributesList(attributesList, filterExpression, populationInfo.eventDescriptor, populationInfo.typeRegistry)
      if (shouldSumPopulation) {
        getSumOfVidRanges(vidRanges)
      } else {
        0L
      }
    }
  }

  /** Returns the total sum given a list of VID ranges. */
  private fun getSumOfVidRanges(vidRanges: List<VidRange>): Long {
    return vidRanges.sumOf { it.endVidInclusive - it.startVid + 1 }
  }

  /**
   * Returns a [Boolean] representing whether the attributes in the list pass a check against
   * the filter expression after being run through a program.
   */
  private fun isValidAttributesList(attributeList: List<Any>, filterExpression: String, eventDescriptor: Descriptor, typeRegistry: TypeRegistry): Boolean {
    // CEL program that will check the event against the filter expression
    val program = EventFilters.compileProgram(eventDescriptor, filterExpression)
    val eventMessage = DynamicMessage.newBuilder(eventDescriptor)

    // Populate event message that will be used in the program
    attributeList.forEach {attribute ->
      val attributeDescriptor = typeRegistry.getDescriptorForTypeUrl(attribute.typeUrl)

      // Create the attribute message by converting the type `Any` to the type specified in the descriptor.
      val attributeMessage = createAttributeMessage(attribute, attributeDescriptor)

      // Ensure attribute is a field in the event descriptor.
      if(!isAttributeFieldInEvent(attributeMessage, eventDescriptor)){
        throw InvalidSpecException("Attribute is not part of Event Descriptor")
      }

      // Validate attribute message with descriptor from type registry.
      if(!isValidAttribute(attributeMessage, attributeDescriptor)){
        throw InvalidSpecException("Invalid Subpopulation Attribute")
      }

      // Find corresponding field descriptor for this attribute.
      val fieldDescriptor = eventDescriptor.fields.first {eventField ->
        eventField.messageType.name === attributeDescriptor.name
      }

      // Set field in event message with typed attribute message.
      eventMessage.setField(fieldDescriptor, attributeMessage)
    }

    return EventFilters.matches(eventMessage.build(), program)
  }

  private fun isAttributeFieldInEvent(attributeMessage: Message, eventDescriptor: Descriptor): Boolean {
    return eventDescriptor.fields.any {
      it.messageType.name === attributeMessage.descriptorForType.name
    }
  }

  /**
   *  Returns a [Boolean] representing whether an attribute contains the correct field options when compared
   *  against those in the attribute descriptor
   * */
  private fun isValidAttribute(attributeMessage: Message, attributeDescriptor: Descriptor): Boolean {
    val attributeFieldOptions = attributeDescriptor.fields.associateBy { it.options }

    // Checks all the field options in the attribute message against those required in the attribute descriptor.
    // If the field option does not exist in the attribute descriptor, the attribute is not valid.
    attributeMessage.allFields.forEach {
      if(!attributeFieldOptions.contains(it.key.options)){
        return false
      }
    }

    return true
  }

  private fun createAttributeMessage(attribute: Any, attributeDescriptor: Descriptor): Message {
    val attributeMessage = DynamicMessage.parseFrom(attributeDescriptor, attribute.toByteString()).toBuilder()

    attributeDescriptor.fields.forEach {
      attributeMessage.setField(it, attribute.unpackSameTypeAs(attributeMessage.build()).getField(it))
    }

    return attributeMessage.build()
  }
}
