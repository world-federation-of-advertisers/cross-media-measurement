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

import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import java.security.cert.X509Certificate
import java.util.logging.Level
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.populations.testing.PopulationBucket
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
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
  private val modelRolloutsStub: ModelRolloutsCoroutineStub,
  private val modelReleasesStub: ModelReleasesCoroutineStub,
  private val populationBucketsList: List<PopulationBucket>,
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

        val modelRelease = getModelRelease(measurementSpec)

        val requisitionFilterExpression = requisitionSpec.population.filter.expression

        fulfillPopulationMeasurement(
          requisition,
          requisitionSpec,
          measurementSpec,
          populationBucketsList,
          modelRelease,
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
    populationBucketsList: List<PopulationBucket>,
    modelRelease: ModelRelease,
    filterExpression: String,
  ) {
    // Filters populationBucketsList through a CEL program and sums the result.
    val populationSum =
      getTotalPopulation(requisitionSpec, populationBucketsList, modelRelease, filterExpression)

    // Create measurement result with sum of valid populations.
    val measurementResult =
      MeasurementKt.result {
        population = MeasurementKt.ResultKt.population { value = populationSum }
      }

    // Fulfill the measurement.
    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  private fun getTotalPopulation(
    requisitionSpec: RequisitionSpec,
    populationBucketsList: List<PopulationBucket>,
    modelRelease: ModelRelease,
    filterExpression: String,
  ): Long {
    val requisitionIntervalEndTime = requisitionSpec.population.interval.endTime

    // Filter populationBucketsList to only include buckets that 1) contain the latest ModelRelease
    // connected to the ModelLine provided in the measurementSpec and 2) have a start and end time
    // within the time window provided in the requisitionSpec.
    val validPopulationBucketsList =
      populationBucketsList.filter {
        it.modelReleasesList.contains(modelRelease.name) &&
          Timestamps.compare(it.validStartTime, requisitionIntervalEndTime) < 0 &&
          (it.validEndTime === null || Timestamps.compare(it.validEndTime, it.validStartTime) > 0)
      }
    return validPopulationBucketsList.sumOf {
      val program = EventFilters.compileProgram(TestEvent.getDescriptor(), filterExpression)

      // Use program to check if Person field in the PopulationBucket contains the filterExpression.
      if (EventFilters.matches(it.event, program)) {
        it.populationSize
      } else {
        0L
      }
    }
  }
}
