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

package org.wfanet.measurement.edpaggregator.dataavailability

import java.time.LocalDate

/** An availability delivery whose output owner has been invalidated or superseded. */
class ObsoleteDataAvailabilityWorkItemException :
  Exception("Managed availability owner is obsolete")

/** Whether a managed marker can describe the current labeled output. */
enum class ManagedDoneObjectDisposition {
  CURRENT,
  FINALIZING,
  OBSOLETE,
}

fun interface ManagedDoneObjectResolver {
  /** Finds the full registered model name for a managed marker without labeled sidecars. */
  suspend fun modelLineFor(eventDate: LocalDate, blobKey: String): String =
    error("Managed trigger model line requires authoritative lookup")

  suspend fun resolve(
    modelLine: String,
    eventDate: LocalDate,
    blobKey: String,
  ): ManagedDoneObjectDisposition
}
