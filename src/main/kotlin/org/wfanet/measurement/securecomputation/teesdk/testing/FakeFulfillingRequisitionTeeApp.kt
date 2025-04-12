// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.teesdk.testing

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Parser
import java.io.File
import java.io.FileOutputStream
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerConfig
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.encryptedMessage

class FakeFulfillingRequisitionTeeApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsCoroutineStub,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  override suspend fun runWork(message: Any) {
    val params = message.unpack(ResultsFulfillerConfig::class.java)
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = params.connectionDetails.clientCert.toTempFile(),
        privateKeyFile = params.connectionDetails.clientPrivateKey.toTempFile(),
        trustedCertCollectionFile = params.connectionDetails.clientCollectionCert.toTempFile(),
      )
    val publicChannel =
      buildMutualTlsChannel(
          params.connectionDetails.kingdomPublicApiTarget,
          clientCerts,
          params.connectionDetails.kingdomPublicApiCertHost,
        )
        .withShutdownTimeout(
          Duration.ofSeconds(
            System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
              ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
          )
        )
    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    requisitionsStub.fulfillDirectRequisition(
      fulfillDirectRequisitionRequest {
        name = "some-requisition"
        encryptedResult = encryptedMessage {}
        certificate = "some-certificate"
      }
    )
  }

  fun ByteString.toTempFile(prefix: String = "temp", suffix: String = ".tmp"): File {
    val byteArray = this.toByteArray()
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.deleteOnExit() // delete the file when the JVM exits
    val fos = FileOutputStream(tempFile)
    fos.write(byteArray)
    fos.close()
    return tempFile
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
  }
}
