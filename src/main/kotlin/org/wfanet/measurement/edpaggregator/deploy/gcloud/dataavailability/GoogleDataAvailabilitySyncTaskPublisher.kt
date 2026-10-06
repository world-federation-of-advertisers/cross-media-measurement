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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskPublisher
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskNotification
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTaskNotification

/** Publishes data availability task notifications to Google Cloud Pub/Sub. */
class GoogleDataAvailabilitySyncTaskPublisher(
  projectId: String,
  private val topicId: String,
  googlePubSubClient: GooglePubSubClient,
) : DataAvailabilitySyncTaskPublisher {
  private val publisher =
    Publisher<DataAvailabilitySyncTaskNotification>(projectId, googlePubSubClient)

  override suspend fun publish(taskName: String) {
    publisher.publishMessage(
      topicId,
      dataAvailabilitySyncTaskNotification { dataAvailabilitySyncTask = taskName },
    )
  }
}
