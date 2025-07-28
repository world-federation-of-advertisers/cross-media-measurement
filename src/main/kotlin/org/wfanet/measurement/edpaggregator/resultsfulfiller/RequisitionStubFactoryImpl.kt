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

import java.io.File
import java.nio.file.Paths
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

class RequisitionStubFactoryImpl(
  private val cmmsCertHost: String?,
  private val cmmsTarget: String,
  private val channelShutdownTimeout: Duration = Duration.ofSeconds(3),
  private val trustedCertCollection: File,
) : RequisitionStubFactory {

  override fun buildRequisitionsStub(
    fulfillerParams: ResultsFulfillerParams
  ): RequisitionsCoroutineStub {
    val publicChannel = run {
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile =
            checkNotNull(
                getRuntimePath(Paths.get(fulfillerParams.cmmsConnection.clientCertResourcePath))
              )
              .toFile(),
          privateKeyFile =
            checkNotNull(
                getRuntimePath(
                  Paths.get(fulfillerParams.cmmsConnection.clientPrivateKeyResourcePath)
                )
              )
              .toFile(),
          trustedCertCollectionFile = trustedCertCollection,
        )
      buildMutualTlsChannel(cmmsTarget, signingCerts, cmmsCertHost)
        .withShutdownTimeout(channelShutdownTimeout)
    }
    return RequisitionsCoroutineStub(publicChannel)
  }
}
