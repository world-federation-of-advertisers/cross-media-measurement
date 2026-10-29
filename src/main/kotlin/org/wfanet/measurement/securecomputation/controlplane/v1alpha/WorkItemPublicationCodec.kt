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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import java.io.InputStream
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem

/** Decodes the internal WorkItem representation published to processing queues. */
object WorkItemPublicationCodec {
  fun parse(input: InputStream): WorkItem = InternalWorkItem.parseFrom(input).toWorkItem()
}
