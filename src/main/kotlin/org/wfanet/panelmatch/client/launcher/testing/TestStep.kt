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

package org.wfanet.panelmatch.client.launcher.testing

import com.google.protobuf.ByteString
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.common.toByteString
import org.wfanet.panelmatch.protocol.common.DeterministicCommutativeCipher

val MP_0_SECRET_KEY: ByteString = "random-mp-string-0".toByteString()

val JOIN_KEYS =
  listOf(
    "some joinkey0".toByteString(),
    "some joinkey1".toByteString(),
    "some joinkey2".toByteString(),
    "some joinkey3".toByteString(),
    "some joinkey4".toByteString()
  )

val SINGLE_BLINDED_KEYS =
  listOf(
    "some single-blinded key0".toByteString(),
    "some single-blinded key1".toByteString(),
    "some single-blinded key2".toByteString(),
    "some single-blinded key3".toByteString(),
    "some single-blinded key4".toByteString()
  )
val DOUBLE_BLINDED_KEYS =
  listOf(
    "some double-blinded key0".toByteString(),
    "some double-blinded key1".toByteString(),
    "some double-blinded key2".toByteString(),
    "some double-blinded key3".toByteString(),
    "some double-blinded key4".toByteString()
  )
val LOOKUP_KEYS =
  listOf(
    "some lookup0".toByteString(),
    "some lookup1".toByteString(),
    "some lookup2".toByteString(),
    "some lookup3".toByteString(),
    "some lookup4".toByteString()
  )

fun buildMockCryptor(): DeterministicCommutativeCipher {
  val mockCryptor: DeterministicCommutativeCipher = mock()
  whenever(mockCryptor.encrypt(any(), any())).thenReturn(SINGLE_BLINDED_KEYS)
  whenever(mockCryptor.reEncrypt(any(), any())).thenReturn(DOUBLE_BLINDED_KEYS)
  whenever(mockCryptor.decrypt(any(), any())).thenReturn(LOOKUP_KEYS)
  return mockCryptor
}

fun buildStep(
  stepType: ExchangeWorkflow.Step.StepCase,
  privateInputLabels: Map<String, String> = emptyMap(),
  privateOutputLabels: Map<String, String> = emptyMap(),
  sharedInputLabels: Map<String, String> = emptyMap(),
  sharedOutputLabels: Map<String, String> = emptyMap(),
  intersectMaxSize: Int = 0,
  intersectMinimumOverlap: Float = 0.0f
): ExchangeWorkflow.Step {
  return ExchangeWorkflow.Step.newBuilder()
    .putAllPrivateInputLabels(privateInputLabels)
    .putAllPrivateOutputLabels(privateOutputLabels)
    .putAllSharedInputLabels(sharedInputLabels)
    .putAllSharedOutputLabels(sharedOutputLabels)
    .apply {
      when (stepType) {
        ExchangeWorkflow.Step.StepCase.INPUT_STEP ->
          inputStep = ExchangeWorkflow.Step.InputStep.getDefaultInstance()
        ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP ->
          encryptStep = ExchangeWorkflow.Step.EncryptStep.getDefaultInstance()
        ExchangeWorkflow.Step.StepCase.REENCRYPT_STEP ->
          reencryptStep = ExchangeWorkflow.Step.ReEncryptStep.getDefaultInstance()
        ExchangeWorkflow.Step.StepCase.DECRYPT_STEP ->
          decryptStep = ExchangeWorkflow.Step.DecryptStep.getDefaultInstance()
        ExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP ->
          intersectAndValidateStep =
            ExchangeWorkflow.Step.IntersectAndValidateStep.newBuilder()
              .apply {
                maxSize = intersectMaxSize
                minimumOverlap = intersectMinimumOverlap
              }
              .build()
        else -> error("Unsupported step config")
      }
    }
    .build()
}
