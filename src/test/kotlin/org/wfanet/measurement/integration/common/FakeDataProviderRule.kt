// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

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
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler

/**
 * JUnit rule for spawning fake DataProvider jobs that attempt to fulfill all their requisitions.
 */
class FakeDataProviderRule : TestRule {
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
    val publicKeyCache = mutableMapOf<String, CombinedPublicKey>()
    var lastPageToken = ""

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
        val resourceKey = metricRequisition.combinedPublicKey
        val combinedPublicKey = publicKeyCache.getOrPut(resourceKey.combinedPublicKeyId) {
          publisherDataStub.getCombinedPublicKey(resourceKey)
        }
        publisherDataStub.uploadMetricValue(
          makeMetricValueFlow(combinedPublicKey.encryptionKey, metricRequisition)
        )
      }
    }
  }

  private fun makeMetricValueFlow(
    combinedPublicKey: ElGamalPublicKey,
    metricRequisition: MetricRequisition
  ): Flow<UploadMetricValueRequest> = flow {
    emit(
      UploadMetricValueRequest.newBuilder().apply {
        headerBuilder.apply {
          key = metricRequisition.key
        }
      }.build()
    )

    emit(
      UploadMetricValueRequest.newBuilder().apply {
        chunkBuilder.data = generateFakeEncryptedSketch(combinedPublicKey)
      }.build()
    )
  }

  private fun generateFakeEncryptedSketch(combinedElGamalKey: ElGamalPublicKey): ByteString {
    val sketch = Sketch.newBuilder().apply {
      config = sketchConfig
      // Adds 7 normal registers with count 1
      for (i in 1L..7L) {
        addRegistersBuilder().apply {
          index = i
          addValues(i)
          addValues(1)
        }
      }
      // Adds 3 normal registers with count 2
      for (i in 11L..13L) {
        addRegistersBuilder().apply {
          index = i
          addValues(i)
          addValues(2)
        }
      }
      // Adds another locally destroyed register.
      addRegistersBuilder().apply {
        index = 10
        addValues(-1)
        addValues(1) // this value doesn't matter
      }
    }.build()
    val request = EncryptSketchRequest.newBuilder().apply {
      this.sketch = sketch
      curveId = combinedElGamalKey.ellipticCurveId.toLong()
      maximumValue = 10
      elGamalKeysBuilder.apply {
        generator = combinedElGamalKey.generator
        element = combinedElGamalKey.element
      }
      // TODO: choose strategy according to the protocol type.
      destroyedRegisterStrategy = FLAGGED_KEY
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

  private suspend fun PublisherDataCoroutineStub.getCombinedPublicKey(
    resourceKey: CombinedPublicKey.Key
  ): CombinedPublicKey {
    return getCombinedPublicKey(
      GetCombinedPublicKeyRequest.newBuilder().apply {
        key = resourceKey
      }.build()
    )
  }

  companion object {
    init {
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
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
