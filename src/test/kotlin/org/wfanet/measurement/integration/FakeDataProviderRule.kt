package org.wfanet.measurement.integration

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.duchy.testing.TestKeys

/**
 * JUnit rule for spawning fake DataProvider jobs that attempt to fulfill all their requisitions.
 */
class FakeDataProviderRule(private val globalCombinedPublicKeyId: String) : TestRule {
  private var jobs = mutableListOf<Job>()

  fun startDataProviderForCampaign(
    externalDataProviderId: ExternalId,
    externalCampaignId: ExternalId,
    publisherDataStub: PublisherDataCoroutineStub
  ) {
    jobs.add(
      GlobalScope.launch {
        val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(250))
        var lastPageToken = ""

        // TODO: get CombinedPublicKey from publisherDataStub
        val combinedPublicKey = CombinedPublicKey.newBuilder().apply {
          keyBuilder.combinedPublicKeyId = globalCombinedPublicKeyId
          publicKey = with(TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY) { elGamalG.concat(elGamalY) }
        }.build()

        while (true) {
          throttler.onReady {
            val response = publisherDataStub.listMetricRequisitions(
              ListMetricRequisitionsRequest.newBuilder().apply {
                parentBuilder.apply {
                  dataProviderId = externalDataProviderId.apiId.value
                  campaignId = externalCampaignId.apiId.value
                }
                filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
                pageToken = lastPageToken
                pageSize = 1
              }.build()
            )
            lastPageToken = response.nextPageToken
            for (metricRequisition in response.metricRequisitionsList) {
              publisherDataStub.uploadMetricValue(
                makeMetricValueFlow(combinedPublicKey, metricRequisition)
              )
            }
          }
        }
      }
    )
  }

  private fun makeMetricValueFlow(
    combinedPublicKey: CombinedPublicKey,
    metricRequisition: MetricRequisition
  ): Flow<UploadMetricValueRequest> = flow {
    emit(
      UploadMetricValueRequest.newBuilder().apply {
        headerBuilder.apply {
          key = metricRequisition.key
          this.combinedPublicKey = combinedPublicKey.key
        }
      }.build()
    )

    emit(
      UploadMetricValueRequest.newBuilder().apply {
        chunkBuilder.data = ByteString.copyFromUtf8("TODO")
      }.build()
    )
  }

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        try {
          base.evaluate()
        } finally {
          runBlocking { jobs.forEach { logAndSuppressExceptionSuspend { it.cancelAndJoin() } } }
        }
      }
    }
  }
}
