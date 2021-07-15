// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.resourcesetup

import com.google.protobuf.ByteString
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer

interface ResourceSetup {

  suspend fun createDataProvider(dataProviderContent: EntityContent): DataProvider

  suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent
  ): MeasurementConsumer
}

/** Relevant data required to create entity like EDP or MC. */
data class EntityContent(
  val displayName: String,
  val consentSignalPrivateKeyDer: ByteString,
  val consentSignalCertificateDer: ByteString,
  // The ASN.1 SubjectPublicKeyInfo in DER format
  val encryptionPublicKeyDer: ByteString
)
