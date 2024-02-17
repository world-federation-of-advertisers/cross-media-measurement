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

package org.wfanet.panelmatch.client.storage.aws.s3

import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.beam.BeamOptions
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.withPrefix
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

/** [StorageFactory] for [S3StorageClient]. */
class S3StorageFactory(
  private val storageDetails: StorageDetails,
  private val exchangeDateKey: ExchangeDateKey,
) : StorageFactory {

  override fun build(options: PipelineOptions?): StorageClient {
    if (options == null) {
      return build()
    }
    val beamOptions = options.`as`(BeamOptions::class.java)
    @Suppress("USELESS_ELVIS") // Beam returns String?
    val accessKey = beamOptions.awsAccessKey ?: ""
    @Suppress("USELESS_ELVIS") // Beam returns String?
    val secretAccessKey = beamOptions.awsSecretAccessKey ?: ""
    @Suppress("USELESS_ELVIS") // Beam returns String?
    val sessionToken = beamOptions.awsSessionToken ?: ""
    if (accessKey.isEmpty() || secretAccessKey.isEmpty() || sessionToken.isEmpty()) {
      return build()
    }
    val builtCredentials = AwsSessionCredentials.create(accessKey, secretAccessKey, sessionToken)
    return S3StorageClient(
        S3AsyncClient.builder()
          .region(Region.of(storageDetails.aws.region))
          .credentialsProvider(StaticCredentialsProvider.create(builtCredentials))
          .build(),
        storageDetails.aws.bucket,
      )
      .withPrefix(exchangeDateKey.path)
  }

  override fun build(): StorageClient {
    if (storageDetails.aws.role.roleArn.isEmpty()) {
      return S3StorageClient(
          S3AsyncClient.builder().region(Region.of(storageDetails.aws.region)).build(),
          storageDetails.aws.bucket,
        )
        .withPrefix(exchangeDateKey.path)
    } else {
      val client: StsClient = StsClient.builder().build()
      val assumeRoleRequestBuilder =
        AssumeRoleRequest.builder()
          .roleArn(storageDetails.aws.role.roleArn)
          .roleSessionName(storageDetails.aws.role.roleSessionName)
      val assumeRoleRequest: AssumeRoleRequest =
        if (storageDetails.aws.role.roleExternalId.isEmpty()) {
          assumeRoleRequestBuilder.build()
        } else {
          assumeRoleRequestBuilder.externalId(storageDetails.aws.role.roleExternalId).build()
        }

      return S3StorageClient(
          S3AsyncClient.builder()
            .region(Region.of(storageDetails.aws.region))
            .credentialsProvider(
              StsAssumeRoleCredentialsProvider.builder()
                .stsClient(client)
                .refreshRequest(assumeRoleRequest)
                .build()
            )
            .build(),
          storageDetails.aws.bucket,
        )
        .withPrefix(exchangeDateKey.path)
    }
  }
}
