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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.type.Interval
import java.util.concurrent.ForkJoinPool
import java.util.logging.Logger
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Production-focused orchestrator for requisition fulfillment.
 * Handles concurrent fulfillment of multiple requisitions with proper error isolation.
 * 
 * This orchestrator takes pre-computed frequency vectors and fulfills requisitions concurrently.
 */
class PipelineOrchestrator {
  
  companion object {
    private val logger = Logger.getLogger(PipelineOrchestrator::class.java.name)
  }
  
  /**
   * Runs concurrent fulfillment for production requisitions.
   * 
   * @param frequencyVectors Map of filter specs to their computed frequency vectors
   * @param requisitionSpecs List of requisitions to fulfill concurrently
   * @param fulfillmentContext Context containing fulfillment dependencies
   * @return Map of requisition names to fulfillment results
   */
  suspend fun fulfill(
    frequencyVectors: Map<FilterSpec, FrequencyVector>,
    requisitionSpecs: List<RequisitionSpec>,
    fulfillmentContext: FulfillmentContext
  ): Map<String, FulfillmentResult> {
    logger.info("Starting concurrent fulfillment for ${requisitionSpecs.size} requisitions")
    
    val threadPool = createThreadPool(8) // Default thread pool size
    val dispatcher = threadPool.asCoroutineDispatcher()

    try {
      return runConcurrentFulfillment(frequencyVectors, requisitionSpecs, fulfillmentContext, dispatcher)
    } finally {
      shutdownThreadPool(threadPool)
    }
  }
  
  /**
   * Runs concurrent fulfillment for production requisitions.
   */
  private suspend fun runConcurrentFulfillment(
    frequencyVectors: Map<FilterSpec, FrequencyVector>,
    requisitionSpecs: List<RequisitionSpec>,
    fulfillmentContext: FulfillmentContext,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Map<String, FulfillmentResult> {
    val fulfillmentStartTime = System.currentTimeMillis()
    
    logger.info("Starting concurrent fulfillment for ${requisitionSpecs.size} requisitions")
    
    // Run all fulfillments concurrently
    val fulfillmentResults = withContext(dispatcher) {
      requisitionSpecs.map { requisition ->
        async {
          try {
            fulfillRequisition(requisition, frequencyVectors, fulfillmentContext)
          } catch (e: Exception) {
            logger.warning("Fulfillment failed for requisition ${requisition.name}: ${e.message}")
            FulfillmentResult(requisition.name, success = false, error = e.message)
          }
        }
      }.awaitAll()
    }
    
    val fulfillmentEndTime = System.currentTimeMillis()
    val fulfillmentDuration = fulfillmentEndTime - fulfillmentStartTime
    
    // Display fulfillment summary
    val successCount = fulfillmentResults.count { it.success }
    val failureCount = fulfillmentResults.size - successCount
    
    logger.info("Fulfillment completed in ${fulfillmentDuration}ms:")
    logger.info("  Successful: $successCount")
    logger.info("  Failed: $failureCount")
    
    if (failureCount > 0) {
      logger.warning("Failed requisitions:")
      fulfillmentResults.filter { !it.success }.forEach { result ->
        logger.warning("  ${result.requisitionName}: ${result.error}")
      }
    }
    
    return fulfillmentResults.associateBy { it.requisitionName }
  }
  
  /**
   * Fulfills a single requisition using DirectMeasurementFulfiller.
   */
  private suspend fun fulfillRequisition(
    requisition: RequisitionSpec,
    frequencyVectors: Map<FilterSpec, FrequencyVector>,
    fulfillmentContext: FulfillmentContext
  ): FulfillmentResult {
    logger.info("Fulfilling requisition: ${requisition.name}")
    
    // Find frequency vectors for this requisition
    val requisitionVectors = frequencyVectors.filter { (filterSpec, _) ->
      requisition.eventGroups.any { it.referenceId == filterSpec.eventGroupReferenceId }
    }
    
    if (requisitionVectors.isEmpty()) {
      return FulfillmentResult(requisition.name, success = false, error = "No matching frequency vectors found")
    }
    
    // Combine frequency vectors for this requisition
    val combinedVector = requisitionVectors.values.reduce { acc, vector -> acc.merge(vector) }
    
    // Create DirectMeasurementFulfiller
    val fulfiller = org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller(
      requisitionName = requisition.name,
      dataProviderCertificateName = fulfillmentContext.dataProviderCertificateName,
      measurementResult = null, // Will be computed by fulfiller
      nonce = requisition.nonce,
      measurementEncryptionPublicKey = requisition.encryptionPublicKey,
      frequencyVector = combinedVector,
      directProtocolConfig = requisition.protocolConfig,
      directNoiseMechanism = requisition.noiseMechanism,
      requisitionsStub = fulfillmentContext.requisitionsStub,
      dataProviderSigningKeyHandle = fulfillmentContext.signingKeyHandle,
      dataProviderCertificateKey = fulfillmentContext.certificateKey
    )
    
    // Fulfill the requisition
    fulfiller.fulfill()
    
    logger.info("Successfully fulfilled requisition: ${requisition.name}")
    return FulfillmentResult(requisition.name, success = true)
  }
  
  private fun createThreadPool(size: Int): ForkJoinPool {
    logger.info("Creating shared work-stealing thread pool with max size: $size")
    
    return ForkJoinPool(
      size,
      { pool ->
        val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        thread.name = "PipelinePool-${thread.poolIndex}"
        thread.isDaemon = false
        thread
      },
      { t, e ->
        logger.severe("Uncaught exception in thread pool: ${t.name} - ${e.message}")
        e.printStackTrace()
      },
      true // async mode for better throughput
    )
  }
  
  private fun shutdownThreadPool(threadPool: ForkJoinPool) {
    logger.info("Shutting down thread pool...")
    threadPool.shutdown()
    if (!threadPool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
      logger.warning("Thread pool did not terminate gracefully, forcing shutdown")
      threadPool.shutdownNow()
    }
    logger.info("Thread pool shut down successfully")
  }
  
}

/**
 * Specification for a requisition to be fulfilled.
 */
data class RequisitionSpec(
  val name: String,
  val eventGroups: List<EventGroupSpec>,
  val nonce: Long,
  val encryptionPublicKey: EncryptionPublicKey,
  val protocolConfig: ProtocolConfig,
  val noiseMechanism: DirectNoiseMechanism
)

/**
 * Specification for an event group within a requisition.
 */
data class EventGroupSpec(
  val referenceId: String,
  val celExpression: String,
  val collectionInterval: Interval,
  val vidSamplingStart: Long,
  val vidSamplingWidth: Long
)

/**
 * Context containing dependencies needed for fulfillment.
 */
data class FulfillmentContext(
  val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  val signingKeyHandle: SigningKeyHandle,
  val certificateKey: DataProviderCertificateKey,
  val dataProviderCertificateName: String
)

/**
 * Result of a fulfillment operation.
 */
data class FulfillmentResult(
  val requisitionName: String,
  val success: Boolean,
  val error: String? = null
)