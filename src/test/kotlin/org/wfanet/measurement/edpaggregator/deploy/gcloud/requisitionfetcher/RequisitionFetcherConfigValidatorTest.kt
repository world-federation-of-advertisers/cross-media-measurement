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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TextFormat
import com.google.protobuf.duration
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.config.edpaggregator.DataProviderRequisitionConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionWorkItemDispatchConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import picocli.CommandLine

class RequisitionFetcherConfigValidatorTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `deployment config with excluded direct namespace is valid`() {
    RequisitionFetcherConfigValidator.validate(
      validFetcherConfig(),
      CONTROL_PLANE_TARGET,
      dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
    )
  }

  @Test
  fun `absent requisition refusal duration uses 48 hour default`() {
    assertThat(RequisitionFetcherConfigValidator.requisitionRefusalDuration(validFetcherConfig()))
      .isEqualTo(java.time.Duration.ofHours(48))
  }

  @Test
  fun `configured requisition refusal duration is returned`() {
    val config =
      validFetcherConfig()
        .toBuilder()
        .setRequisitionRefusalDuration(duration { seconds = 6 * 60 * 60 })
        .build()

    assertThat(RequisitionFetcherConfigValidator.requisitionRefusalDuration(config))
      .isEqualTo(java.time.Duration.ofHours(6))
  }

  @Test
  fun `deployment config rejects non-positive requisition refusal duration`() {
    val config = validFetcherConfig().toBuilder().setRequisitionRefusalDuration(duration {}).build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          config,
          CONTROL_PLANE_TARGET,
          dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
        )
      }

    assertThat(exception).hasMessageThat().contains("must be positive")
  }

  @Test
  fun `deployment config rejects invalid requisition refusal duration`() {
    val config =
      validFetcherConfig()
        .toBuilder()
        .setRequisitionRefusalDuration(
          duration {
            seconds = 1
            nanos = 1_000_000_000
          }
        )
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          config,
          CONTROL_PLANE_TARGET,
          dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
        )
      }

    assertThat(exception).hasMessageThat().contains("Invalid 'requisition_refusal_duration'")
  }

  @Test
  fun `deployment config rejects DataWatcher matching direct namespace`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          validFetcherConfig(),
          CONTROL_PLANE_TARGET,
          dataWatcherConfig("^gs://bucket/(.*)$"),
        )
      }

    assertThat(exception).hasMessageThat().contains("matches direct-dispatch prefix")
  }

  @Test
  fun `deployment config requires direct dispatch for every data provider`() {
    val config =
      RequisitionFetcherConfig.newBuilder()
        .addConfigs(validDataProviderConfig().toBuilder().clearWorkItemDispatch())
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          config,
          CONTROL_PLANE_TARGET,
          dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
        )
      }

    assertThat(exception).hasMessageThat().contains("Missing 'work_item_dispatch'")
  }

  @Test
  fun `deployment config requires control plane target`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          validFetcherConfig(),
          "",
          dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
        )
      }

    assertThat(exception).hasMessageThat().contains("control-plane target")
  }

  @Test
  fun `deployment config requires requisition metadata storage connection`() {
    val config =
      RequisitionFetcherConfig.newBuilder()
        .addConfigs(
          validDataProviderConfig().toBuilder().clearRequisitionMetadataStorageConnection()
        )
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        RequisitionFetcherConfigValidator.validate(
          config,
          CONTROL_PLANE_TARGET,
          dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
        )
      }

    assertThat(exception)
      .hasMessageThat()
      .contains("Missing 'requisition_metadata_storage_connection'")
  }

  @Test
  fun `deployment config rejects non-canonical storage prefixes`() {
    for (prefix in listOf("/direct", "direct/")) {
      val config =
        RequisitionFetcherConfig.newBuilder()
          .addConfigs(
            validDataProviderConfig().toBuilder().setWorkItemDispatch(
              validDataProviderConfig().workItemDispatch.toBuilder().setStoragePathPrefix(prefix)
            )
          )
          .build()

      val exception =
        assertFailsWith<IllegalArgumentException> {
          RequisitionFetcherConfigValidator.validate(
            config,
            CONTROL_PLANE_TARGET,
            dataWatcherConfig("^gs://bucket/legacy/(.*)$"),
          )
        }

      assertThat(exception).hasMessageThat().contains("must not start or end with '/'")
    }
  }

  @Test
  fun `CLI validates textproto files`() {
    val fetcherFile = temporaryFolder.newFile("requisition-fetcher-config.textproto")
    fetcherFile.writeText(TextFormat.printer().printToString(validFetcherConfig()))
    val watcherFile = temporaryFolder.newFile("data-watcher-config.textproto")
    watcherFile.writeText(
      TextFormat.printer().printToString(dataWatcherConfig("^gs://bucket/legacy/(.*)$"))
    )

    val exitCode =
      CommandLine(ValidateRequisitionFetcherConfig())
        .execute(
          "--requisition-fetcher-config=${fetcherFile.path}",
          "--data-watcher-config=${watcherFile.path}",
          "--control-plane-target=$CONTROL_PLANE_TARGET",
        )

    assertThat(exitCode).isEqualTo(0)
  }

  private fun validFetcherConfig(): RequisitionFetcherConfig =
    RequisitionFetcherConfig.newBuilder().addConfigs(validDataProviderConfig()).build()

  private fun validDataProviderConfig(): DataProviderRequisitionConfig =
    DataProviderRequisitionConfig.newBuilder()
      .setDataProvider(DATA_PROVIDER)
      .setRequisitionStorage(
        StorageParams.newBuilder()
          .setGcs(StorageParams.GcsStorage.newBuilder().setBucketName("bucket"))
      )
      .setStoragePathPrefix("legacy")
      .setEdpPrivateKeyPath("/secrets/edp.key")
      .setCmmsConnection(tlsParams())
      .setRequisitionMetadataStorageConnection(tlsParams())
      .setWorkItemDispatch(
        RequisitionWorkItemDispatchConfig.newBuilder()
          .setStoragePathPrefix("direct")
          .setControlPlaneConnection(tlsParams())
          .setQueue("results-fulfiller-queue")
          .setResultsFulfillerParams(resultsFulfillerParams())
      )
      .build()

  private fun tlsParams(): TransportLayerSecurityParams =
    TransportLayerSecurityParams.newBuilder()
      .setCertFilePath("/secrets/client.pem")
      .setPrivateKeyFilePath("/secrets/client.key")
      .setCertCollectionFilePath("/secrets/roots.pem")
      .build()

  private fun resultsFulfillerParams(): ResultsFulfillerParams =
    ResultsFulfillerParams.newBuilder()
      .setDataProvider(DATA_PROVIDER)
      .setStorageParams(
        ResultsFulfillerParams.StorageParams.newBuilder()
          .setLabeledImpressionsBlobDetailsUriPrefix("gs://bucket/impressions")
      )
      .setConsentParams(
        ResultsFulfillerParams.ConsentParams.newBuilder()
          .setResultCsCertDerResourcePath("cert.der")
          .setResultCsPrivateKeyDerResourcePath("private-key.der")
          .setPrivateEncryptionKeyResourcePath("encryption-key.tink")
          .setEdpCertificateName("dataProviders/dp/certificates/cert")
      )
      .setCmmsConnection(
        org.wfanet.measurement.edpaggregator.v1alpha.TransportLayerSecurityParams.newBuilder()
          .setClientCertResourcePath("client.pem")
          .setClientPrivateKeyResourcePath("client.key")
      )
      .setNoiseParams(
        ResultsFulfillerParams.NoiseParams.newBuilder()
          .setNoiseType(ResultsFulfillerParams.NoiseParams.NoiseType.NONE)
      )
      .build()

  private fun dataWatcherConfig(regex: String): DataWatcherConfig =
    DataWatcherConfig.newBuilder()
      .addWatchedPaths(
        WatchedPath.newBuilder()
          .setIdentifier("legacy-requisitions")
          .setSourcePathRegex(regex)
          .setControlPlaneQueueSink(
            WatchedPath.ControlPlaneQueueSink.newBuilder().setQueue("results-fulfiller-queue")
          )
      )
      .build()

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val CONTROL_PLANE_TARGET = "secure-computation.example:8443"
  }
}
