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
import org.wfanet.panelmatch.protocol.common.Cryptor

val MP_0_SECRET_KEY: ByteString = ByteString.copyFromUtf8("random-mp-string-0")

val JOIN_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some joinkey0"),
    ByteString.copyFromUtf8("some joinkey1"),
    ByteString.copyFromUtf8("some joinkey2"),
    ByteString.copyFromUtf8("some joinkey3"),
    ByteString.copyFromUtf8("some joinkey4")
  )

val SINGLE_BLINDED_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some single-blinded key0"),
    ByteString.copyFromUtf8("some single-blinded key1"),
    ByteString.copyFromUtf8("some single-blinded key2"),
    ByteString.copyFromUtf8("some single-blinded key3"),
    ByteString.copyFromUtf8("some single-blinded key4")
  )
val DOUBLE_BLINDED_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some double-blinded key0"),
    ByteString.copyFromUtf8("some double-blinded key1"),
    ByteString.copyFromUtf8("some double-blinded key2"),
    ByteString.copyFromUtf8("some double-blinded key3"),
    ByteString.copyFromUtf8("some double-blinded key4")
  )
val LOOKUP_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some lookup0"),
    ByteString.copyFromUtf8("some lookup1"),
    ByteString.copyFromUtf8("some lookup2"),
    ByteString.copyFromUtf8("some lookup3"),
    ByteString.copyFromUtf8("some lookup4")
  )

fun buildMockCryptor(): Cryptor {
  val mockCryptor: Cryptor = mock()
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
