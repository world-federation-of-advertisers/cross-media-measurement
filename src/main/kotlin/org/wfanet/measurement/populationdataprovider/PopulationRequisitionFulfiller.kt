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
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import io.grpc.Status
import io.grpc.StatusException
import java.security.cert.X509Certificate
import java.util.logging.Level
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DeterministicCount
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.size
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.dataprovider.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Data class associated with a population.
 *
 * @param populationSpec The [PopulationSpec] that contains 1) information on the attributes of a
 *   population, and 2) vid ranges that are used to calculate the size of the population.
 * @param eventMessageDescriptor The [Descriptor] of the event message that wraps the event template
 *   which is used by the CEL program to filter out irrelevant populations and calculate the size.
 *   The event template should contain the same types provided in the attributes list of the
 *   [PopulationSpec].
 */
data class PopulationInfo(
  val populationSpec: PopulationSpec,
  // TODO(jojijac0b): Dynamically generate an Event message type for each EventTemplate by building
  //  a DescriptorProto.
  val eventMessageDescriptor: Descriptor,
)

/** A requisition fulfiller for PDP businesses. */
class PopulationRequisitionFulfiller(
  pdpData: DataProviderData,
  certificatesStub: CertificatesCoroutineStub,
  requisitionsStub: RequisitionsCoroutineStub,
  throttler: Throttler,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val modelRolloutsStub: ModelRolloutsCoroutineStub,
  private val modelReleasesStub: ModelReleasesCoroutineStub,
  private val populationInfoMap: Map<PopulationKey, PopulationInfo>,
  private val typeRegistry: TypeRegistry,
) :
  RequisitionFulfiller(
    pdpData,
    certificatesStub,
    requisitionsStub,
    throttler,
    trustedCertificates,
  ) {

  /** A sequence of operations done in the simulator. */
  override suspend fun run() {
    throttler.loopOnReady { executeRequisitionFulfillingWorkflow() }
  }

  /** Executes the requisition fulfillment workflow. */
  override suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisitions = getRequisitions()

    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      try {
        logger.info("Processing requisition ${requisition.name}...")

        // TODO(@SanjayVas): Verify that DataProvider public key in Requisition matches private key
        //  in pdpData. A real PDP would look up the matching private key.

        val measurementConsumerCertificate: Certificate =
          getCertificate(requisition.measurementConsumerCertificate)

        val (measurementSpec, requisitionSpec) =
          try {
            verifySpecifications(requisition, measurementConsumerCertificate)
          } catch (e: InvalidConsentSignalException) {
            logger.log(Level.WARNING, e) {
              "Consent signaling verification failed for ${requisition.name}"
            }
            throw RequisitionRefusalException.Default(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              e.message.orEmpty(),
            )
          }

        logger.log(Level.INFO, "MeasurementSpec:\n$measurementSpec")
        logger.log(Level.INFO, "RequisitionSpec:\n$requisitionSpec")

        val populationInfo: PopulationInfo = getPopulationInfo(measurementSpec)
        PopulationSpecValidator.validateVidRangesList(populationInfo.populationSpec).getOrThrow()

        val requisitionFilterExpression = requisitionSpec.population.filter.expression

        fulfillPopulationMeasurement(
          requisition,
          requisitionSpec,
          measurementSpec,
          requisitionFilterExpression,
          populationInfo,
          typeRegistry,
        )
      } catch (e: RequisitionRefusalException) {
        if (e !is RequisitionRefusalException.Test) {
          logger.log(Level.WARNING, e) { "Refusing Requisition ${requisition.name}" }
        }

        refuseRequisition(requisition.name, e.justification, e.message!!, requisition.etag)
      }
    }
  }

  private suspend fun getPopulationInfo(measurementSpec: MeasurementSpec): PopulationInfo {
    val modelRelease: ModelRelease = getModelRelease(measurementSpec)

    val populationId: PopulationKey =
      requireNotNull(PopulationKey.fromName(modelRelease.population)) {
        throw InvalidSpecException(
          "Measurement spec model line does not contain a valid Population for the model release of its latest model rollout."
        )
      }

    return populationInfoMap.getValue(populationId)
  }

  /**
   * Returns the [ModelRelease] associated with the latest `ModelRollout` that is connected to the
   * `ModelLine` provided in the MeasurementSpec`
   */
  private suspend fun getModelRelease(measurementSpec: MeasurementSpec): ModelRelease {
    // TODO(@jojijac0b): Handle case where measurement spans across one or more model outages.
    //  Should use HoldbackModelLine in this case to reflect what is done with measurement reports.

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

  /** Fulfills a population measurement. */
  private suspend fun fulfillPopulationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    filterExpression: String,
    populationInfo: PopulationInfo,
    typeRegistry: TypeRegistry,
  ) {

    val operativeFields = getPopulationOperativeFields(populationInfo.eventMessageDescriptor)

    // CEL program that will check the event against the filter expression
    val program: Program =
      EventFilters.compileProgram(
        populationInfo.eventMessageDescriptor,
        filterExpression,
        operativeFields,
      )

    // Create measurement result with sum of valid populations.
    val measurementResult: Measurement.Result =
      MeasurementKt.result {
        population =
          MeasurementKt.ResultKt.population {
            value = computePopulation(populationInfo, program, typeRegistry)
            deterministicCount = DeterministicCount.getDefaultInstance()
          }
      }

    // Fulfill the measurement.
    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  /**
   * Returns a [Set] of operative fields derived from a [Descriptor]. Only fields that have the
   * population attribute set to true will be returned.
   */
  private fun getPopulationOperativeFields(eventMessageDescriptor: Descriptor): Set<String> {
    // TODO(jojijac0b): Pass in specific template descriptor instead of entire event message
    //  descriptor.
    return eventMessageDescriptor.fields
      .flatMap { templateField ->
        templateField.messageType.fields.map { templateFieldDescriptor ->
          if (
            templateFieldDescriptor.options
              .getExtension(EventAnnotationsProto.templateField)
              .populationAttribute
          ) {
            "${templateField.name}.${templateFieldDescriptor.name}"
          } else null
        }
      }
      .filterNotNull()
      .toSet()
  }

  companion object {
    /**
     * Computes population using the "deterministic count" methodology by filtering the
     * populationBucketsList through a CEL program and summing the result.
     */
    fun computePopulation(
      populationInfo: PopulationInfo,
      program: Program,
      typeRegistry: TypeRegistry,
    ): Long {
      return populationInfo.populationSpec.subpopulationsList.sumOf {
        val attributesList = it.attributesList
        val vidRanges = it.vidRangesList
        val shouldSumPopulation =
          isValidAttributesList(
            attributesList,
            populationInfo.eventMessageDescriptor,
            program,
            typeRegistry,
          )
        if (shouldSumPopulation) {
          vidRanges.sumOf { jt -> jt.size() }
        } else {
          0L
        }
      }
    }

    /**
     * Returns a [Boolean] representing whether the attributes in the list are 1) the correct type
     * and
     * 2) pass a check against the filter expression after being run through a CEL program.
     */
    private fun isValidAttributesList(
      attributeList: List<Any>,
      eventMessageDescriptor: Descriptor,
      program: Program,
      typeRegistry: TypeRegistry,
    ): Boolean {
      // Event message that will be passed to CEL program
      val eventMessage: DynamicMessage.Builder = DynamicMessage.newBuilder(eventMessageDescriptor)

      // Populate event message that will be used in the program if attribute is valid
      attributeList.forEach { attribute ->
        val attributeDescriptor: Descriptor =
          typeRegistry.getDescriptorForTypeUrl(attribute.typeUrl)
        val requiredAttributes: List<FieldDescriptor> =
          attributeDescriptor.fields.filter {
            it.options.getExtension(EventAnnotationsProto.templateField).populationAttribute
          }

        // Create the attribute message of the type specified in attribute descriptor using the type
        // registry.
        val descriptor: Descriptors.Descriptor =
          typeRegistry.getDescriptorForTypeUrl(attribute.typeUrl)
        val attributeMessage: DynamicMessage = DynamicMessage.parseFrom(descriptor, attribute.value)

        // If the attribute type is not a field in the event message, it is not valid.
        val isAttributeFieldInEvent =
          eventMessageDescriptor.fields.any {
            it.messageType.name === attributeMessage.descriptorForType.name
          }
        require(isAttributeFieldInEvent) {
          throw RequisitionFulfiller.InvalidSpecException(
            "Subpopulation attribute is not a field in the event descriptor."
          )
        }

        // If the population_attribute option in the attribute message is set to true, we do not
        // allow
        // the value to be unspecified.
        val isValidAttribute = attributeMessage.allFields.keys.containsAll(requiredAttributes)
        require(isValidAttribute) {
          throw RequisitionFulfiller.InvalidSpecException(
            "Subpopulation population attribute cannot be unspecified."
          )
        }

        // Find corresponding field descriptor for this attribute.
        val fieldDescriptor: Descriptors.FieldDescriptor =
          eventMessageDescriptor.fields.first { eventField ->
            eventField.messageType.name === attributeDescriptor.name
          }

        // Set field in event message with typed attribute message.
        eventMessage.setField(fieldDescriptor, attributeMessage)
      }
      return EventFilters.matches(eventMessage.build(), program)
    }
  }
}
