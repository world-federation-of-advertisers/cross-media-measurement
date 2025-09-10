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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import java.io.BufferedReader
import java.time.Duration
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import java.util.logging.Logger
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityConfig
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailability
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import java.io.File
import java.time.Clock
import org.wfanet.measurement.storage.SelectedStorageClient

class DataAvailabilityFunction() : HttpFunction {

    override fun service(request: HttpRequest, response: HttpResponse) {
        logger.fine("Starting DataAvailabilityFunction")
        val requestBody: BufferedReader = request.getReader()
        val dataAvailabilityConfig =
          DataAvailabilityConfig.newBuilder()
            .apply { JsonFormat.parser().merge(requestBody, this) }
            .build()

        // Read the path as request header
        val path = request.getFirstHeader(DATA_WATHCER_PATH_HEADER).orElseThrow { IllegalArgumentException("Missing required header: $DATA_WATHCER_PATH_HEADER") }

        val doneBlobUri =
            SelectedStorageClient.parseBlobUri(path)

        val storageClient =
            SelectedStorageClient(
                blobUri = doneBlobUri,
                rootDirectory =
                    if (dataAvailabilityConfig.dataAvailabilityStorage.hasFileSystem())
                        File(checkNotNull(fileSystemStorageRoot))
                    else null,
                projectId = dataAvailabilityConfig.dataAvailabilityStorage.gcs.projectId
            )

        val cmmsSigningCerts =
            SigningCerts.fromPemFiles(
                certificateFile = checkNotNull(File(dataAvailabilityConfig.cmmsConnection.certFilePath)),
                privateKeyFile = checkNotNull(File(dataAvailabilityConfig.cmmsConnection.privateKeyFilePath)),
                trustedCertCollectionFile =
                checkNotNull(File(dataAvailabilityConfig.cmmsConnection.certCollectionFilePath)),
            )
        val cmmsPublicChannel =
            buildMutualTlsChannel(kingdomTarget, cmmsSigningCerts, kingdomCertHost)
                .withShutdownTimeout(kingdomChannelShutdownDuration)

        val impressionMetadataStorageSigningCerts =
            SigningCerts.fromPemFiles(
                certificateFile = checkNotNull(File(dataAvailabilityConfig.impressionMetadataStorageConnection.certFilePath)),
                privateKeyFile = checkNotNull(File(dataAvailabilityConfig.impressionMetadataStorageConnection.privateKeyFilePath)),
                trustedCertCollectionFile =
                checkNotNull(File(dataAvailabilityConfig.impressionMetadataStorageConnection.certCollectionFilePath)),
            )
        val impressionMetadataStoragePublicChannel =
            buildMutualTlsChannel(impressionMetadataTarget, impressionMetadataStorageSigningCerts, impressionMetadataCertHost)
                .withShutdownTimeout(impressionMetadataChannelShutdownDuration)

        val dataProvidersClient = DataProvidersCoroutineStub(cmmsPublicChannel)
        val impressionMetadataServicesClient = ImpressionMetadataServiceCoroutineStub(impressionMetadataStoragePublicChannel)

        val dataAvailability = DataAvailability(
            storageClient,
            dataProvidersClient,
            impressionMetadataServicesClient,
            dataAvailabilityConfig.dataProvider,
            MinimumIntervalThrottler(Clock.systemUTC(), throttlerDuration)
        )

        runBlocking { dataAvailability.sync(path) }

    }

    companion object {
        private val logger: Logger = Logger.getLogger(this::class.java.name)

        private const val CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
        private const val THROTTLER_DURATION_MILLIS = 1000L
        private val throttlerDuration =
            Duration.ofMillis(System.getenv("THROTTLER_MILLIS")?.toLong() ?: THROTTLER_DURATION_MILLIS)

        private const val DATA_WATHCER_PATH_HEADER: String = "X-DataWatcher-Path"

        private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
        private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
        private val kingdomChannelShutdownDuration =
            Duration.ofSeconds(
                System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
                    ?: CHANNEL_SHUTDOWN_DURATION_SECONDS
            )

        private val impressionMetadataTarget = EnvVars.checkNotNullOrEmpty("IMPRESSION_METADATA_TARGET")
        private val impressionMetadataCertHost: String? = System.getenv("IMPRESSION_METADATA_CERT_HOST")
        private val impressionMetadataChannelShutdownDuration =
            Duration.ofSeconds(
                System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
                    ?: CHANNEL_SHUTDOWN_DURATION_SECONDS
            )

        private val fileSystemStorageRoot = System.getenv("FILE_STORAGE_ROOT")
    }
}