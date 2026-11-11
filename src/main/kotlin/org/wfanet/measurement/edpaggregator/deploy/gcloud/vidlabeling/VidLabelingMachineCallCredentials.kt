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

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.IdTokenProvider
import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.Status
import java.io.IOException
import java.net.URI
import java.util.concurrent.Executor

/** Audience-scoped Google ID tokens obtained from the function's service account. */
class VidLabelingMachineCallCredentials
internal constructor(private val audience: String, credentialsProvider: () -> IdTokenCredentials) :
  CallCredentials() {
  constructor(
    audience: String
  ) : this(
    audience,
    {
      val provider =
        GoogleCredentials.getApplicationDefault() as? IdTokenProvider
          ?: throw IllegalArgumentException(
            "Application Default Credentials cannot provide ID tokens"
          )
      IdTokenCredentials.newBuilder()
        .setIdTokenProvider(provider)
        .setTargetAudience(audience)
        .build()
    },
  )

  private val credentials by lazy(credentialsProvider)

  override fun applyRequestMetadata(
    requestInfo: RequestInfo,
    appExecutor: Executor,
    applier: MetadataApplier,
  ) {
    appExecutor.execute {
      try {
        val metadata = Metadata()
        for ((key, values) in credentials.getRequestMetadata(URI(audience))) {
          val metadataKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)
          for (value in values) metadata.put(metadataKey, value)
        }
        applier.apply(metadata)
      } catch (e: IOException) {
        applier.fail(
          Status.UNAVAILABLE.withDescription("Google ID-token credentials are unavailable")
        )
      } catch (e: IllegalArgumentException) {
        applier.fail(
          Status.FAILED_PRECONDITION.withDescription(
            "Google ID-token credentials are not configured"
          )
        )
      } catch (e: RuntimeException) {
        applier.fail(
          Status.UNAVAILABLE.withDescription("Google ID-token credentials are unavailable")
        )
      }
    }
  }
}
