/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.config

import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage

/** EventGroup metadata for testing. */
object EventGroupMetadata {
  /** Returns a [TestMetadataMessage] for the specified [publisherId]. */
  fun testMetadata(publisherId: Int) = testMetadataMessage { this.publisherId = publisherId }
}
