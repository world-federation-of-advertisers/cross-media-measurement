// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.eventpreprocessing.testing

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.eventpreprocessing.EncryptEventsDoFn
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedDeterministicCommutativeCipherKeyProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedHkdfPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedIdentifierHashPepperProvider
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.compressionParameters

private const val IDENTIFIER_HASH_PEPPER = "<some-identifier-hash-pepper>"
private const val HKDF_HASH_PEPPER = "<some-hkdf-hash-pepper>"
private const val CRYPTO_KEY = "<some-crypto-key>"
private const val EXPECTED_SUFFIX = "$IDENTIFIER_HASH_PEPPER$HKDF_HASH_PEPPER$CRYPTO_KEY"

/** Unit tests for [EncryptEventsDoFn]. */
@RunWith(JUnit4::class)
class FakeEncryptEventsDoFnTest : BeamTestBase() {
  @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
  private val coder: Coder<MutableList<KV<ByteString, ByteString>>> =
    ListCoder.of(KvCoder.of(ByteStringCoder.of(), ByteStringCoder.of()))

  @Test
  fun encrypt() {
    val arbitraryUnprocessedEvents: MutableList<KV<ByteString, ByteString>> =
      mutableListOf(inputOf("1000", "2"), inputOf("2000", "4"))
    val collection = pcollectionOf("collection1", arbitraryUnprocessedEvents, coder = coder)

    val compressionParameters =
      pcollectionViewOf("Create CompressionParameters", compressionParameters {})

    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>> =
      EncryptEventsDoFn(
        FakeEventPreprocessor(),
        HardCodedIdentifierHashPepperProvider(IDENTIFIER_HASH_PEPPER.toByteStringUtf8()),
        HardCodedHkdfPepperProvider(HKDF_HASH_PEPPER.toByteStringUtf8()),
        HardCodedDeterministicCommutativeCipherKeyProvider(CRYPTO_KEY.toByteStringUtf8()),
        compressionParameters,
      )

    assertThat(collection.apply(ParDo.of(doFn).withSideInputs(compressionParameters)))
      .containsInAnyOrder(expectedOutputOf(1001, "2"), expectedOutputOf(2001, "4"))
  }
}

private fun inputOf(key: String, value: String): KV<ByteString, ByteString> {
  return kvOf(key.toByteStringUtf8(), value.toByteStringUtf8())
}

private fun outputOf(key: Long, value: String): KV<Long, ByteString> {
  return kvOf(key, value.toByteStringUtf8())
}

private fun expectedOutputOf(key: Long, value: String): KV<Long, ByteString> {
  return outputOf(key, value + EXPECTED_SUFFIX)
}
