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

import com.google.protobuf.timestamp
import com.google.type.interval
import io.netty.handler.ssl.ClientAuth
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.config.edpaggregator.dataAvailabilityConfig
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger

@RunWith(JUnit4::class)
class DataAvailabilityFunctionTest {

    private lateinit var grpcServer: CommonServer
    private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

    private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
        onBlocking { replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>()) }
            .thenAnswer {
                DataProvider.getDefaultInstance()
            }
    }

    private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
        onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
            .thenAnswer { invocation ->
                val request = invocation.getArgument<ListImpressionMetadataRequest>(0)

                // Build some fake proto data for the response
                listImpressionMetadataResponse {
                    impressionMetadata += listOf(
                        impressionMetadata {
                            name = "${request.parent}/impressionMetadata1"
                            modelLine = request.filter.modelLine
                            blobUri = "gs://bucket/blob-1"
                            interval = interval {
                                startTime = timestamp { seconds = 100 }
                                endTime = timestamp { seconds = 200 }
                            }
                            state = ImpressionMetadata.State.ACTIVE
                        },
                        impressionMetadata {
                            name = "${request.parent}/impressionMetadata2"
                            modelLine = request.filter.modelLine
                            blobUri = "gs://bucket/blob-2"
                            interval = interval {
                                startTime = timestamp { seconds = 200 }
                                endTime = timestamp { seconds = 300 }
                            }
                            state = ImpressionMetadata.State.ACTIVE
                        }
                    )

                }
            }
    }

    @get:Rule
    val grpcTestServerRule = GrpcTestServerRule {
        addService(dataProvidersServiceMock)
        addService(impressionMetadataServiceMock)
    }

    @get:Rule val tempFolder = TemporaryFolder()

    @Before
    fun startInfra() {
        /** Start gRPC server with mock EventGroups service */
        grpcServer =
            CommonServer.fromParameters(
                verboseGrpcLogging = true,
                certs = serverCerts,
                clientAuth = ClientAuth.REQUIRE,
                nameForLogging = "DataAvailabilityServers",
                services = listOf(dataProvidersServiceMock.bindService(), impressionMetadataServiceMock.bindService()),
            )
                .start()
        functionProcess =
            FunctionsFrameworkInvokerProcess(
                javaBinaryPath = FUNCTION_BINARY_PATH,
                classTarget = GCG_TARGET,
            )
        logger.info("Started gRPC server on port ${grpcServer.port}")
    }

    @After
    fun cleanUp() {
        grpcServer.shutdown()
    }

    @Test
    fun `sync registersUnregisteredImpressionMetadata`() {
        val dataAvailabilityConfig = dataAvailabilityConfig {
            dataProvider = "dataProviders/edp123"
            cmmsConnection = transportLayerSecurityParams {
                certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
                privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
                certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
            }
            impressionMetadataStorageConnection = transportLayerSecurityParams {
                certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
                privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
                // TODO(@marcopremier): Replace with ImpressionMetadata cert when available
                certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
            }
            dataAvailabilityStorage = storageParams { fileSystem = fileSystemStorage {} }
        }

        // DO_NOT_SUBMIT (Complete tests once the ImpressionMetadataStorage is implemented)
        val port = runBlocking {
            functionProcess.start(
                mapOf(
                    "FILE_STORAGE_ROOT" to tempFolder.root.toString(),
                    "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
                    "KINGDOM_CERT_HOST" to "localhost",
                    "KINGDOM_SHUTDOWN_DURATION_SECONDS" to "3",
                )
            )
        }

    }

    companion object {

        private val SECRETS_DIR: Path =
            getRuntimePath(
                Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
            )!!
        private val serverCerts =
            SigningCerts.fromPemFiles(
                certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
                privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
                trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem").toFile(),
            )
        private val logger: Logger = Logger.getLogger(this::class.java.name)

        private val FUNCTION_BINARY_PATH =
            Paths.get(
                "wfa_measurement_system",
                "src",
                "main",
                "kotlin",
                "org",
                "wfanet",
                "measurement",
                "edpaggregator",
                "deploy",
                "gcloud",
                "dataavailability",
                "testing",
                "InvokeDataAvailabilityFunction",
            )
        private const val GCG_TARGET =
            "org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilityFunction"
    }

}