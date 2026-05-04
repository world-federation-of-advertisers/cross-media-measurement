/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator

abstract class AbstractEdpAggregatorCorrectnessTest(
  private val measurementSystem: MeasurementSystem
) {

  private val mcSimulator: MeasurementConsumerSimulator
    get() = measurementSystem.mcSimulator

  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS:
    ((EventGroup) -> Boolean)?
  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB: ((EventGroup) -> Boolean)?

  // TODO(@marcopremier): Enable HMMS tests by adding a new EDP

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency(
        "1233",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachOnly(
        "1234",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create incremental direct reach only measurements in same report and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create N incremental direct reach and frequency measurements and
      // verify its result.
      mcSimulator.testDirectReachOnly(
        runId = "1235",
        numMeasurements = 3,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its
      // result.
      mcSimulator.testImpression(
        "1236",
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a TrusTee reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachOnly(
        "1237",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB,
      )
    }

  @Test
  fun `create a TrusTee RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachAndFrequency(
        "1238",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB,
      )
    }

  @Test
  fun `EDPA EventGroup with non-default entity_type round-trips via the CMMS public API`() =
    runBlocking {
      val response =
        measurementSystem.publicEventGroupsStub
          .withAuthenticationKey(measurementSystem.apiAuthenticationKey)
          .listEventGroups(
            listEventGroupsRequest {
              parent = measurementSystem.measurementConsumerName
              pageSize = 100
              // Default entity_type_in is ["campaign"], which would hide the ad_group EventGroup.
              filter =
                ListEventGroupsRequestKt.filter {
                  entityTypeIn += "campaign"
                  entityTypeIn += "ad_group"
                }
            }
          )

      val byRefId = response.eventGroupsList.associateBy { it.eventGroupReferenceId }
      val adGroup: EventGroup = byRefId.getValue(AD_GROUP_EDP_EVENT_GROUP_REF_ID)
      assertThat(adGroup.entityKey.entityType).isEqualTo("ad_group")
      assertThat(adGroup.entityKey.entityId).isEqualTo(AD_GROUP_EDP_EVENT_GROUP_REF_ID)
      assertThat(adGroup.eventGroupMetadata.entityMetadata.fieldsMap).containsKey("placement")
    }

  @Test
  fun `EDPA EventGroup without entity_key defaults to campaign with no entity_id or metadata`() =
    runBlocking {
      val response =
        measurementSystem.publicEventGroupsStub
          .withAuthenticationKey(measurementSystem.apiAuthenticationKey)
          .listEventGroups(
            listEventGroupsRequest {
              parent = measurementSystem.measurementConsumerName
              pageSize = 100
              // No filter — server defaults entity_type_in to ["campaign"].
            }
          )

      val legacy: EventGroup =
        response.eventGroupsList.single {
          it.eventGroupReferenceId == EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID
        }
      // Schema column DEFAULT "campaign"; EntityId stays NULL; entity_metadata not set.
      assertThat(legacy.entityKey.entityType).isEqualTo("campaign")
      assertThat(legacy.entityKey.entityId).isEmpty()
      assertThat(legacy.eventGroupMetadata.hasEntityMetadata()).isFalse()
    }

  @Test
  fun `default ListEventGroups filter hides non-campaign EventGroups`() = runBlocking {
    val response =
      measurementSystem.publicEventGroupsStub
        .withAuthenticationKey(measurementSystem.apiAuthenticationKey)
        .listEventGroups(
          listEventGroupsRequest {
            parent = measurementSystem.measurementConsumerName
            pageSize = 100
            // No filter — server defaults entity_type_in to ["campaign"].
          }
        )

    val refIds = response.eventGroupsList.map { it.eventGroupReferenceId }.toSet()
    assertThat(refIds).contains(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID)
    assertThat(refIds).doesNotContain(AD_GROUP_EDP_EVENT_GROUP_REF_ID)
  }

  interface MeasurementSystem {
    val runId: String
    val mcSimulator: MeasurementConsumerSimulator
    val publicEventGroupsStub: EventGroupsCoroutineStub
    val measurementConsumerName: String
    val apiAuthenticationKey: String
  }

  companion object {
    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    internal const val EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID = "edpa-eg-reference-id-1"
    internal const val AD_GROUP_EDP_EVENT_GROUP_REF_ID = "edpa-eg-reference-id-2"

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }

    val MC_ENCRYPTION_PRIVATE_KEY: PrivateKeyHandle by lazy {
      loadEncryptionPrivateKey(MC_ENCRYPTION_PRIVATE_KEY_NAME)
    }

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    val MEASUREMENT_CONSUMER_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }
  }
}
