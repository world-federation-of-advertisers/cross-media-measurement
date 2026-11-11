/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.Status
import java.io.IOException
import java.util.Base64
import java.util.concurrent.Executor
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class VidLabelingMachineCallCredentialsTest {
  @Test
  fun `signed token metadata is applied exactly once`() {
    val token = tokenWithPayload("""{"exp":4102444800}""")
    val provider = mock<IdTokenProvider>()
    whenever(provider.idTokenWithAudience(any(), anyOrNull())).thenReturn(IdToken.create(token))
    val credentials = credentials(provider)
    val applier = RecordingApplier()

    credentials.applyRequestMetadata(mock(), Executor { it.run() }, applier)

    assertThat(applier.calls).isEqualTo(1)
    assertThat(applier.status).isNull()
    assertThat(applier.metadata!!.get(AUTHORIZATION)).isEqualTo("Bearer $token")
  }

  @Test
  fun `unchecked SDK token parsing failure completes callback with sanitized unavailable`() {
    val provider = mock<IdTokenProvider>()
    // The real SDK throws NullPointerException when a returned ID token has no expiry.
    whenever(provider.idTokenWithAudience(any(), anyOrNull())).thenAnswer {
      IdToken.create(tokenWithPayload("""{"sub":"sensitive-subject"}"""))
    }
    val applier = RecordingApplier()

    credentials(provider).applyRequestMetadata(mock(), Executor { it.run() }, applier)

    assertUnavailable(applier)
  }

  @Test
  fun `credential IO failure completes callback with sanitized unavailable`() {
    val provider = mock<IdTokenProvider>()
    whenever(provider.idTokenWithAudience(any(), anyOrNull())).thenAnswer {
      throw IOException("sensitive credential diagnostic")
    }
    val applier = RecordingApplier()

    credentials(provider).applyRequestMetadata(mock(), Executor { it.run() }, applier)

    assertUnavailable(applier)
  }

  @Test
  fun `unsupported ADC completes callback with sanitized failed precondition`() {
    val credentials =
      VidLabelingMachineCallCredentials(AUDIENCE) {
        throw IllegalArgumentException("sensitive credential diagnostic")
      }
    val applier = RecordingApplier()

    credentials.applyRequestMetadata(mock(), Executor { it.run() }, applier)

    assertThat(applier.calls).isEqualTo(1)
    assertThat(applier.metadata).isNull()
    assertThat(applier.status!!.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(applier.status!!.description)
      .isEqualTo("Google ID-token credentials are not configured")
    assertThat(applier.status!!.cause).isNull()
  }

  private fun credentials(provider: IdTokenProvider) =
    VidLabelingMachineCallCredentials(AUDIENCE) {
      IdTokenCredentials.newBuilder()
        .setIdTokenProvider(provider)
        .setTargetAudience(AUDIENCE)
        .build()
    }

  private fun assertUnavailable(applier: RecordingApplier) {
    assertThat(applier.calls).isEqualTo(1)
    assertThat(applier.metadata).isNull()
    assertThat(applier.status!!.code).isEqualTo(Status.Code.UNAVAILABLE)
    assertThat(applier.status!!.description)
      .isEqualTo("Google ID-token credentials are unavailable")
    assertThat(applier.status!!.cause).isNull()
  }

  private class RecordingApplier : CallCredentials.MetadataApplier() {
    var calls = 0
    var metadata: Metadata? = null
    var status: Status? = null

    override fun apply(headers: Metadata) {
      calls++
      metadata = headers
    }

    override fun fail(failure: Status) {
      calls++
      status = failure
    }
  }

  companion object {
    private const val AUDIENCE = "https://system-api.example/vid-labeling-machine-api"
    private val AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

    private fun tokenWithPayload(payload: String): String {
      val encoder = Base64.getUrlEncoder().withoutPadding()
      val header = encoder.encodeToString("""{"alg":"RS256"}""".toByteArray())
      return "$header.${encoder.encodeToString(payload.toByteArray())}.c2lnbmF0dXJl"
    }
  }
}
