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

package org.wfanet.measurement.securecomputation.deploy.gcloud.publisher

import com.google.api.core.ApiFutures
import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.protobuf.StringValue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.PublishFailedException

@RunWith(JUnit4::class)
class GoogleWorkItemPublisherTest {
  private val googlePublisher = mock<GooglePublisher>()
  private val publisher =
    GoogleWorkItemPublisher(
      projectId = "test-project",
      googlePubSubClient = mock<GooglePubSubClient>(),
      orderedPublisherFactory = { _, _ -> googlePublisher },
    )

  @Test
  fun `ordered publish resumes ordering key after failure`() = runBlocking {
    whenever(googlePublisher.publish(any()))
      .thenReturn(ApiFutures.immediateFailedFuture(RuntimeException("transient failure")))

    assertFailsWith<PublishFailedException> {
      publisher.publishMessage(
        queueName = "test-queue",
        message = StringValue.of("message"),
        orderingKey = ORDERING_KEY,
      )
    }

    inOrder(googlePublisher) {
      verify(googlePublisher).publish(any())
      verify(googlePublisher).resumePublish(ORDERING_KEY)
    }
  }

  @Test
  fun `ordered publish propagates cancellation without resuming ordering key`() = runBlocking {
    whenever(googlePublisher.publish(any()))
      .thenReturn(ApiFutures.immediateFailedFuture(CancellationException("cancelled")))

    assertFailsWith<CancellationException> {
      publisher.publishMessage(
        queueName = "test-queue",
        message = StringValue.of("message"),
        orderingKey = ORDERING_KEY,
      )
    }

    verify(googlePublisher, never()).resumePublish(any())
  }

  companion object {
    private const val ORDERING_KEY = "serialization-key"
  }
}
