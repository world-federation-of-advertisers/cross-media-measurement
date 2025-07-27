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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventPathResolver

@RunWith(JUnit4::class)
class PathEchoImpressionMetadataServiceTest {

  @Test
  fun `returns one source per day across range`(): Unit = runBlocking {
    val pathResolver = object : EventPathResolver {
      override suspend fun resolvePaths(
        date: LocalDate,
        eventGroupReferenceId: String
      ): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths(
          metadataPath = "file:///meta-bucket/ds/$date/event-group-reference-id/$eventGroupReferenceId/metadata",
          eventGroupReferenceId = eventGroupReferenceId
        )
      }
    }

    val svc = PathEchoImpressionMetadataService(pathResolver, ZoneId.of("UTC"))
    val startDate = LocalDate.of(2025, 1, 10)
    val endDate = LocalDate.of(2025, 1, 12)
    val start = startDate.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val end = endDate.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant()

    val sources = svc.listImpressionDataSources(
      "eg-2",
      interval {
        startTime = timestamp { seconds = start.epochSecond; nanos = start.nano }
        endTime = timestamp { seconds = end.epochSecond; nanos = end.nano }
      }
    )

    assertThat(sources).hasSize(3)
    assertThat(sources.map { it.blobDetails.blobUri }).containsExactly(
      "file:///meta-bucket/ds/2025-01-10/event-group-reference-id/eg-2/metadata",
      "file:///meta-bucket/ds/2025-01-11/event-group-reference-id/eg-2/metadata",
      "file:///meta-bucket/ds/2025-01-12/event-group-reference-id/eg-2/metadata",
    )
  }
}

