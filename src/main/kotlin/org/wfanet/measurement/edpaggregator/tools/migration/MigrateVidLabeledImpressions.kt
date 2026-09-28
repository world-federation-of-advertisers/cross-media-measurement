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

package org.wfanet.measurement.edpaggregator.tools.migration

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import java.time.LocalDate
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.gcloud.kms.GCloudKmsClientFactory
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/** Copies existing encrypted VID-labeled impressions into a new model-line namespace. */
@Command(
  name = "migrate-vid-labeled-impressions",
  description =
    [
      "Copies encrypted VID-labeled impressions and rewrites their BlobDetails for another " +
        "model line."
    ],
  mixinStandardHelpOptions = true,
)
class MigrateVidLabeledImpressions : Runnable {
  @Option(
    names = ["--start-date"],
    description = ["First source date to migrate, inclusive (YYYY-MM-DD)."],
    required = true,
  )
  private lateinit var startDate: LocalDate

  @Option(
    names = ["--end-date"],
    description = ["Last source date to migrate, inclusive (YYYY-MM-DD)."],
    required = true,
  )
  private lateinit var endDate: LocalDate

  @Option(
    names = ["--source-model-line"],
    description = ["ModelLine resource name expected in every source BlobDetails."],
    required = true,
  )
  private lateinit var sourceModelLine: String

  @Option(
    names = ["--destination-model-line"],
    description = ["ModelLine resource name to write into destination BlobDetails."],
    required = true,
  )
  private lateinit var destinationModelLine: String

  @Option(
    names = ["--source-date-prefix"],
    description = ["Absolute gs:// prefix whose immediate child directories are source dates."],
    required = true,
  )
  private lateinit var sourceDatePrefix: String

  @Option(
    names = ["--destination-blob-prefix"],
    description =
      [
        "Absolute gs:// VID-labeled output prefix. Output is written below " +
          "model-line/{destination-id}/{date}."
      ],
    required = true,
  )
  private lateinit var destinationBlobPrefix: String

  @set:Option(
    names = ["--model-lines-are-compatible"],
    description =
      [
        "Required acknowledgement that source and destination assignments are compatible across " +
          "the migration boundary."
      ],
    required = true,
  )
  private var modelLinesAreCompatible: Boolean by Delegates.notNull()

  @set:Option(
    names = ["--dry-run"],
    description = ["Validate and print planned work without writing objects."],
    defaultValue = "false",
  )
  private var dryRun: Boolean by Delegates.notNull()

  @Option(
    names = ["--gcs-project"],
    description = ["Google Cloud project used for Storage API requests."],
    defaultValue = "",
  )
  private lateinit var gcsProject: String

  @Option(
    names = ["--kms-wif-audience"],
    description = ["GCP Workload Identity Federation audience for the EDP KMS."],
    required = true,
  )
  private lateinit var kmsWifAudience: String

  @Option(
    names = ["--kms-service-account"],
    description = ["EDP service account to impersonate for GCP KMS decryption."],
    required = true,
  )
  private lateinit var kmsServiceAccount: String

  @Option(
    names = ["--kms-credential-source-file"],
    description = ["Path to the Confidential Space attestation token."],
    defaultValue = DEFAULT_CREDENTIAL_SOURCE_FILE,
  )
  private lateinit var kmsCredentialSourceFile: String

  @Option(
    names = ["--storage-api-endpoint"],
    description = ["Google Cloud Storage API endpoint override."],
    hidden = true,
  )
  private var storageApiEndpoint: String? = null

  override fun run() {
    val summary = runBlocking {
      VidLabeledImpressionsMigrator(buildStorage(), ::buildKmsClient, ::println)
        .migrate(
          VidLabeledImpressionsMigrator.Request(
            startDate = startDate,
            endDate = endDate,
            sourceModelLine = sourceModelLine,
            destinationModelLine = destinationModelLine,
            sourceDatePrefix = sourceDatePrefix,
            destinationBlobPrefix = destinationBlobPrefix,
            modelLinesAreCompatible = modelLinesAreCompatible,
            dryRun = dryRun,
          )
        )
    }
    println(summary.toDisplayString())
    check(summary.failedDates == 0) {
      "Migration failed for ${summary.failedDates} date(s); no done marker was written for those dates."
    }
  }

  private fun buildStorage(): Storage {
    val builder = StorageOptions.newBuilder()
    if (gcsProject.isNotEmpty()) {
      builder.setProjectId(gcsProject)
    }
    val apiEndpoint = storageApiEndpoint
    if (apiEndpoint != null) {
      builder.setHost(apiEndpoint)
      builder.setCredentials(NoCredentials.getInstance())
    }
    return builder.build().service
  }

  private fun buildKmsClient(): KmsClient =
    GCloudKmsClientFactory()
      .getKmsClient(
        GCloudWifCredentials(
          audience = kmsWifAudience,
          subjectTokenType = SUBJECT_TOKEN_TYPE,
          tokenUrl = TOKEN_URL,
          credentialSourceFilePath = kmsCredentialSourceFile,
          serviceAccountImpersonationUrl =
            EDP_TARGET_SERVICE_ACCOUNT_FORMAT.format(kmsServiceAccount),
        )
      )

  companion object {
    private const val SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
    private const val TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val DEFAULT_CREDENTIAL_SOURCE_FILE =
      "/run/container_launcher/attestation_verifier_claims_token"
    private const val EDP_TARGET_SERVICE_ACCOUNT_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"
  }
}

fun main(args: Array<String>) = commandLineMain(MigrateVidLabeledImpressions(), args)
