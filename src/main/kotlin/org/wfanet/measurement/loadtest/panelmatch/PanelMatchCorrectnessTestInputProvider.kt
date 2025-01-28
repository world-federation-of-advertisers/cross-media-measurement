/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.panelmatch

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.common.unprocessedEventOf
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.common.compression.CompressionParametersKt
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.toDelimitedByteString

/** Provide utility functions to set up workflows in [PanelMatchSimulator]. */
object PanelMatchCorrectnessTestInputProvider {

  const val HKDF_PEPPER = "some-hkdf-pepper"

  enum class TestType {
    DOUBLE_BLIND,
    MINI_EXCHANGE,
    FULL_WITH_PREPROCESSING,
  }

  fun getInitialDataProviderInputsForTestType(testType: TestType): Map<String, ByteString> {
    when (testType) {
      TestType.DOUBLE_BLIND -> return getInitialDataProviderInputsForDoubleBlind()
      TestType.MINI_EXCHANGE -> return getInitialDataProviderInputsForMiniExchange()
      TestType.FULL_WITH_PREPROCESSING ->
        return getInitialDataProviderInputsForFullWithPreprocessing()
    }
  }

  fun getInitialModelProviderInputsForTestType(testType: TestType): Map<String, ByteString> {
    when (testType) {
      TestType.DOUBLE_BLIND -> return getInitialModelProviderInputsForDoubleBlind()
      TestType.MINI_EXCHANGE -> return getInitialModelProviderInputsForMiniExchange()
      TestType.FULL_WITH_PREPROCESSING ->
        return getInitialModelProviderInputsForFullWithPreprocessing()
    }
  }

  private fun getInitialDataProviderInputsForDoubleBlind(): Map<String, ByteString> {
    val EDP_COMMUTATIVE_DETERMINISTIC_KEY = "some-key".toByteStringUtf8()
    return mapOf("edp-commutative-deterministic-key" to EDP_COMMUTATIVE_DETERMINISTIC_KEY)
  }

  private fun getInitialDataProviderInputsForMiniExchange(): Map<String, ByteString> {
    return mapOf("edp-hkdf-pepper" to HKDF_PEPPER.toByteStringUtf8())
  }

  private fun getInitialDataProviderInputsForFullWithPreprocessing(): Map<String, ByteString> {
    val EDP_COMPRESSION_PARAMETERS = compressionParameters {
      brotli = CompressionParametersKt.brotliCompressionParameters { dictionary = ByteString.EMPTY }
    }
    val EDP_EVENT_DATA_MANIFEST = "edp-event-data-?-of-1".toByteStringUtf8()

    val EDP_DATABASE_ENTRIES: List<UnprocessedEvent> =
      (0 until 100).flatMap { index ->
        listOf(
          unprocessedEventOf(
            "join-key-$index".toByteStringUtf8(),
            "payload-1-for-join-key-$index".toByteStringUtf8(),
          ),
          unprocessedEventOf(
            "join-key-$index".toByteStringUtf8(),
            "payload-2-for-join-key-$index".toByteStringUtf8(),
          ),
        )
      }

    val EDP_EVENT_DATA_BLOB = EDP_DATABASE_ENTRIES.map { it.toDelimitedByteString() }.flatten()
    return mapOf(
      "edp-event-data" to EDP_EVENT_DATA_MANIFEST,
      "edp-event-data-0-of-1" to EDP_EVENT_DATA_BLOB,
      "edp-compression-parameters" to EDP_COMPRESSION_PARAMETERS.toByteString(),
      "edp-previous-single-blinded-join-keys" to ByteString.EMPTY,
    )
  }

  private fun getInitialModelProviderInputsForDoubleBlind(): Map<String, ByteString> {
    val PLAINTEXT_JOIN_KEYS = joinKeyAndIdCollection {
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-1".toByteStringUtf8(), "join-key-id-1".toByteStringUtf8())
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-2".toByteStringUtf8(), "join-key-id-2".toByteStringUtf8())
    }
    return mapOf("mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString())
  }

  private fun getInitialModelProviderInputsForMiniExchange(): Map<String, ByteString> {
    return emptyMap()
  }

  private fun getInitialModelProviderInputsForFullWithPreprocessing(): Map<String, ByteString> {
    val PLAINTEXT_JOIN_KEYS = joinKeyAndIdCollection {
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-1".toByteStringUtf8(), "join-key-id-1".toByteStringUtf8())
      joinKeyAndIds +=
        joinKeyAndIdOf("join-key-2".toByteStringUtf8(), "join-key-id-2".toByteStringUtf8())
    }
    return mapOf("mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString())
  }
}
