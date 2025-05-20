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

package org.wfanet.measurement.loadtest.measurementconsumer

import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery

/**
 * [SyntheticGeneratorEventQuery] that reads [SyntheticEventGroupSpec]s from [EventGroup] metadata.
 */
class MetadataSyntheticGeneratorEventQuery(
  populationSpec: SyntheticPopulationSpec,
  private val mcPrivateKey: PrivateKeyHandle,
) : SyntheticGeneratorEventQuery(populationSpec, TestEvent.getDescriptor()) {
  override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
    val metadata = decryptMetadata(eventGroup.encryptedMetadata, mcPrivateKey)
    return metadata.metadata.unpack(SyntheticEventGroupSpec::class.java)
  }
}
