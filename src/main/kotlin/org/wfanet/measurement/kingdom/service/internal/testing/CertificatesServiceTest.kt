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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest


@RunWith(JUnit4::class)
abstract class CertificatesServiceTest {
  abstract val certificatesService: CertificatesCoroutineImplBase

  @Test
  fun `getCertificate fails for missing Certificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.getCertificate(
          GetCertificateRequest.newBuilder()
            .setExternalCertificateId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
            .build()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `createCertificate succeeds`() = runBlocking {
    val createdCertificate =
      dataProvidersService.createCertificate(
        Certificate.newBuilder().apply { detailsBuilder.apply { apiVersion = "2" } }.build()
      )

    val dataProviderRead =
      dataProvidersService.getCertificate(
        GetCertificateRequest.newBuilder()
          .setExternalCertificateId(
            createdCertificate.externalCertificateId
          )
          .build()
      )

    assertThat(dataProviderRead).isEqualTo(createdCertificate)
  }
}
