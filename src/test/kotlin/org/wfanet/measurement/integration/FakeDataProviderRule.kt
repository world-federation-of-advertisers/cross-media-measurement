// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.integration

import com.google.protobuf.ByteString
import java.nio.file.Paths
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
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.parseTextProto
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
        runDataProvider(externalDataProviderId, externalCampaignId, publisherDataStub)
      }
    )
  }

  private suspend fun runDataProvider(
    externalDataProviderId: ExternalId,
    externalCampaignId: ExternalId,
    publisherDataStub: PublisherDataCoroutineStub
  ) {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(250))
    var lastPageToken = ""

    // TODO: get CombinedPublicKey from publisherDataStub
    val combinedPublicKey = CombinedPublicKey.newBuilder().apply {
      keyBuilder.combinedPublicKeyId = globalCombinedPublicKeyId
      publicKey = with(TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY) { elGamalG.concat(elGamalY) }
    }.build()

    throttler.loopOnReady {
      val request = ListMetricRequisitionsRequest.newBuilder().apply {
        parentBuilder.apply {
          dataProviderId = externalDataProviderId.apiId.value
          campaignId = externalCampaignId.apiId.value
        }
        filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
        pageToken = lastPageToken
        pageSize = 1
      }.build()
      val response = publisherDataStub.listMetricRequisitions(request)
      lastPageToken = response.nextPageToken
      for (metricRequisition in response.metricRequisitionsList) {
        publisherDataStub.uploadMetricValue(
          makeMetricValueFlow(combinedPublicKey, metricRequisition)
        )
      }
    }
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
        chunkBuilder.data = generateFakeEncryptedSketch()
      }.build()
    )
  }

  private fun generateFakeEncryptedSketch(): ByteString {
    val sketch = Sketch.newBuilder().apply {
      config = sketchConfig
      for (i in 1L..10L) {
        addRegistersBuilder().apply {
          index = i
          addValues(i)
          addValues(1)
        }
      }
    }.build()
    val request = EncryptSketchRequest.newBuilder().apply {
      this.sketch = sketch
      curveId = TestKeys.CURVE_ID.toLong()
      maximumValue = 10
      elGamalKeysBuilder.apply {
        elGamalG = TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY.elGamalG
        elGamalY = TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY.elGamalY
      }
    }.build()
    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )
    return response.encryptedSketch
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

  companion object {
    init {
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/anysketch/crypto")
      )
    }

    private val sketchConfig: SketchConfig
    init {
      val configPath =
        "/org/wfanet/measurement/loadtest/config/liquid_legions_sketch_config.textproto"
      val resource = this::class.java.getResource(configPath)

      sketchConfig = resource.openStream().use { input ->
        parseTextProto(input.bufferedReader(), SketchConfig.getDefaultInstance())
      }
    }
  }
}
