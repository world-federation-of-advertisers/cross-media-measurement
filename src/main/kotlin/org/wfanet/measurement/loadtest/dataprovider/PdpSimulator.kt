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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.unpack
import java.security.cert.X509Certificate
import java.util.logging.Level
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpec.VidRange
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** A simulator handling PDP businesses. */
class PdpSimulator(
  pdpData: DataProviderData,
  certificatesStub: CertificatesCoroutineStub,
  dataProvidersStub: DataProvidersCoroutineStub,
  requisitionsStub: RequisitionsCoroutineStub,
  throttler: Throttler,
  trustedCertificates: Map<ByteString, X509Certificate>,
  measurementConsumerName: String,
  private val populationSpecMap: Map<String, PopulationSpec>,
  private val populationId: String
) :
  DataProviderSimulator(
    pdpData,
    certificatesStub,
    dataProvidersStub,
    requisitionsStub,
    throttler,
    trustedCertificates,
    measurementConsumerName
  ) {

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

        // TODO: add check for invalid population id
        val populationSpec = populationSpecMap.getValue(populationId)
        val subPopulationList = populationSpec.subpopulationsList

        val requisitionFilterExpression = requisitionSpec.population.filter.expression

        fulfillPopulationMeasurement(
          requisition,
          requisitionSpec,
          measurementSpec,
          subPopulationList,
          requisitionFilterExpression,
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

  private suspend fun fulfillPopulationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    subPopulationList: List<SubPopulation>,
    filterExpression: String,
  ) {
    // Filters populationBucketsList through a CEL program and sums the result.
    val populationSum = getTotalPopulation(subPopulationList, filterExpression)

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
    subPopulationList: List<SubPopulation>,
    filterExpression: String,
  ): Long {
    return subPopulationList.sumOf { it ->
      val attributesList = it.attributesList
      val vidRanges = it.vidRangesList
      val shouldSumPopulation = isValidAttributesList(attributesList, filterExpression)
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
   * Returns a [Boolean] representing whether any of the attributes in the list pass a check against
   * the filter expression after being run through a program.
   */
  private fun isValidAttributesList(attributeList: List<Any>, filterExpression: String): Boolean {
    val program = EventFilters.compileProgram(TestEvent.getDescriptor(), filterExpression)
    return attributeList.any {
      if (isPerson(it)) {
        val event: TestEvent = testEvent { person = it.unpack<Person>() }
        EventFilters.matches(event, program)
      } else {
        false
      }
    }
  }

  /** Returns a [Boolean] representing whether an attribute is of type Person. */
  private fun isPerson(attribute: Any): Boolean {
    val typeUrl = attribute.typeUrl
    return (typeUrl == ProtoReflection.getTypeUrl(Person.getDescriptor()))
  }
}
