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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.testing

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigs
import kotlin.text.matches
import kotlin.io.path.Path
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import com.google.protobuf.TextFormat
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient

/*
 * Test Data Watcher Function that uses GooglePubSubEmulatorClient.
 */
class TestDataWatcherFunction(override val workItemsService: GooglePubSubWorkItemsService) : DataWatcherFunction() {

  override val schema = "file:///"

}
