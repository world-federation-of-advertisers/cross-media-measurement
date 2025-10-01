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

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import io.grpc.Status
import io.grpc.StatusException
import java.security.cert.X509Certificate
import java.util.logging.Level
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DeterministicCount
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.dataprovider.InvalidRequisitionException
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.dataprovider.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.dataprovider.UnfulfillableRequisitionException
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

/**
 * A requisition fulfiller for a Population Data Provider (PDP).
 *
 * The fulfiller supports a single CMMS instance (market) with a single set of templates.
 */
class PopulationRequisitionFulfiller(
  pdpData: DataProviderData,
  certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub,
  requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  throttler: Throttler,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val modelRolloutsStub: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub,
  private val modelReleasesStub: ModelReleasesGrpcKt.ModelReleasesCoroutineStub,
  private val populationsStub: PopulationsGrpcKt.PopulationsCoroutineStub,
  /** Protobuf descriptor of the event message for the CMMS instance. */
  private val eventMessageDescriptor: Descriptors.Descriptor,
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

        val populationSpec: PopulationSpec = getValidPopulationSpec(measurementSpec)
        fulfillPopulationMeasurement(requisition, requisitionSpec, measurementSpec, populationSpec)
      } catch (e: RequisitionRefusalException) {
        if (e !is RequisitionRefusalException.Test) {
          logger.log(Level.WARNING, e) { "Refusing Requisition ${requisition.name}" }
        }

        refuseRequisition(requisition.name, e.justification, e.message!!, requisition.etag)
      }
    }
  }

  /**
   * Returns the valid [PopulationSpec] for the specified [measurementSpec].
   *
   * @throws RequisitionRefusalException if there was a problem obtaining the valid [PopulationSpec]
   *   that should result in the [Requisition] being refused.
   */
  private suspend fun getValidPopulationSpec(measurementSpec: MeasurementSpec): PopulationSpec {
    val modelRelease: ModelRelease = getModelRelease(measurementSpec.modelLine)
    val population: Population = getPopulation(modelRelease.population)
    if (!population.hasPopulationSpec()) {
      throw UnfulfillableRequisitionException(
        "Population ${population.name} does not have a PopulationSpec"
      )
    }

    return population.populationSpec.also {
      try {
        PopulationSpecValidator.validate(it, eventMessageDescriptor)
      } catch (e: PopulationSpecValidationException) {
        throw UnfulfillableRequisitionException("PopulationSpec is invalid", e)
      }
    }
  }

  private suspend fun getPopulation(populationName: String): Population {
    val populationKey = requireNotNull(PopulationKey.fromName(populationName))
    if (populationKey.parentKey.toName() != dataProviderData.name) {
      throw InvalidRequisitionException("Population is for wrong PDP")
    }

    return try {
      populationsStub.getPopulation(getPopulationRequest { name = populationName })
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND ->
          UnfulfillableRequisitionException("Population $populationName not found", e)
        else -> Exception("Error retrieving Population $populationName", e)
      }
    }
  }

  /**
   * Returns the [ModelRelease] associated with the latest
   * [org.wfanet.measurement.api.v2alpha.ModelRollout] for the specified [modelLineName].
   */
  private suspend fun getModelRelease(modelLineName: String): ModelRelease {
    // TODO(@jojijac0b): Handle case where measurement spans across one or more model outages.
    //  Should use HoldbackModelLine in this case to reflect what is done with measurement reports.

    // Returns list of ModelRollouts.
    val listModelRolloutsResponse =
      try {
        modelRolloutsStub.listModelRollouts(listModelRolloutsRequest { parent = modelLineName })
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            InvalidRequisitionException("ModelLine $modelLineName not found", e)
          else -> UnfulfillableRequisitionException("Error retrieving ModelLine $modelLineName", e)
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
        Status.Code.NOT_FOUND ->
          InvalidRequisitionException("ModelRelease $modelReleaseName not found", e)
        else -> Exception("Error retrieving ModelLine $modelReleaseName", e)
      }
    }
  }

  /** Fulfills a population measurement. */
  private suspend fun fulfillPopulationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
  ) {
    val filterExpression = requisitionSpec.population.filter.expression
    // Create measurement result with sum of valid populations.
    val measurementResult: Measurement.Result =
      MeasurementKt.result {
        this.population =
          MeasurementKt.ResultKt.population {
            value =
              try {
                MeasurementResults.computePopulation(
                  populationSpec,
                  filterExpression,
                  eventMessageDescriptor,
                )
              } catch (e: EventFilterValidationException) {
                throw InvalidRequisitionException("Population filter is invalid", e)
              }
            deterministicCount = DeterministicCount.getDefaultInstance()
          }
      }

    // Fulfill the measurement.
    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }
}
