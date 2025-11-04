// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.daemon.mill.trustee

import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretVersionName
import java.io.File
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.daemon.mill.trustee.TrusTeeMillDaemon
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.kms.GCloudKmsClientFactory
import picocli.CommandLine

private const val SECRET_VERSION = "latest"

@CommandLine.Command(
  name = "GcsTrusTeeMillDaemon",
  description = ["Honest Majority Share Shuffle Mill daemon."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GcsTrusTeeMillDaemon : TrusTeeMillDaemon() {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  @CommandLine.Option(
    names = ["--gcp-project-id"],
    description = ["The Google Cloud Project ID."],
    required = true,
  )
  private lateinit var gcpProjectId: String

  @CommandLine.Option(
    names = ["--tls-cert-secret-id"],
    description = ["Secret ID of the mill's TLS certificate."],
    required = true,
  )
  private lateinit var tlsCertSecretId: String

  @CommandLine.Option(
    names = ["--tls-key-secret-id"],
    description = ["Secret ID of the mill's TLS private key."],
    required = true,
  )
  private lateinit var tlsKeySecretId: String

  @CommandLine.Option(
    names = ["--cert-collection-secret-id"],
    description = ["Secret ID of the trusted root CA collection."],
  )
  private var certCollectionSecretId: String? = null

  @CommandLine.Option(
    names = ["--cs-cert-secret-id"],
    description = ["Secret ID of the consent signaling certificate."],
    required = true,
  )
  private lateinit var csCertSecretId: String

  @CommandLine.Option(
    names = ["--cs-private-key-secret-id"],
    description = ["Secret ID of the consent signaling private key."],
    required = true,
  )
  private lateinit var csPrivateKeySecretId: String

  override fun run() {
    saveCerts()

    val gcs = GcsFromFlags(gcsFlags)
    run(GcsStorageClient.fromFlags(gcs), GCloudKmsClientFactory())
  }

  private fun saveCerts() {
    val tlsCert = accessSecretBytes(gcpProjectId, tlsCertSecretId, SECRET_VERSION)
    saveByteArrayToFile(tlsCert, flags.tlsFlags.certFile.path)

    val tlsKey = accessSecretBytes(gcpProjectId, tlsKeySecretId, SECRET_VERSION)
    saveByteArrayToFile(tlsKey, flags.tlsFlags.privateKeyFile.path)

    val certCollectionSecret = certCollectionSecretId
    val certCollectionFile = flags.tlsFlags.certCollectionFile
    if (certCollectionSecret != null && certCollectionFile != null) {
      val certCollection = accessSecretBytes(gcpProjectId, certCollectionSecret, SECRET_VERSION)
      saveByteArrayToFile(certCollection, certCollectionFile.path)
    }

    val csCert = accessSecretBytes(gcpProjectId, csCertSecretId, SECRET_VERSION)
    saveByteArrayToFile(csCert, flags.csCertificateDerFile.path)

    val csPrivateKey = accessSecretBytes(gcpProjectId, csPrivateKeySecretId, SECRET_VERSION)
    saveByteArrayToFile(csPrivateKey, flags.csPrivateKeyDerFile.path)
  }

  private fun saveByteArrayToFile(bytes: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    file.writeBytes(bytes)
  }

  private fun accessSecretBytes(projectId: String, secretId: String, version: String): ByteArray {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request =
        AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build()

      val response = client.accessSecretVersion(request)
      response.payload.data.toByteArray()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(GcsTrusTeeMillDaemon(), args)
