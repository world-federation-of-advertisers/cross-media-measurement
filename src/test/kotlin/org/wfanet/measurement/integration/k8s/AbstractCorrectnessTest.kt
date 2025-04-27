/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

import io.grpc.Channel
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PolicyKey
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.getPolicyRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator

/** Test for correctness of the CMMS on Kubernetes. */
abstract class AbstractCorrectnessTest(private val measurementSystem: MeasurementSystem) {
  private val runId: String
    get() = measurementSystem.runId

  private val testHarness: MeasurementConsumerSimulator
    get() = measurementSystem.testHarness

  private val reportingTestHarness: ReportingUserSimulator
    get() = measurementSystem.reportingTestHarness

  @Test(timeout = 1 * 60 * 1000)
  fun `impression measurement completes with expected result`() = runBlocking {
    testHarness.testImpression("$runId-impression")
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `duration measurement completes with expected result`() = runBlocking {
    testHarness.testDuration("$runId-duration")
  }

  @Test
  fun `HMSS reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.testReachAndFrequency(
      "$runId-hmss-reach-and-freq",
      DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
    )
  }

  @Test
  fun `LLv2 reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.testReachAndFrequency(
      "$runId-llv2-reach-and-freq",
      DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
    )
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `report can be created`() = runBlocking {
    reportingTestHarness.testCreateReport("$runId-test-report")
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `basic report can be retrieved`() = runBlocking {
    reportingTestHarness.testGetBasicReport("$runId-test-basic-report")
  }

  interface MeasurementSystem {
    val runId: String
    val testHarness: MeasurementConsumerSimulator
    val reportingTestHarness: ReportingUserSimulator
  }

  companion object {
    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }

    val KINGDOM_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MEASUREMENT_CONSUMER_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val REPORTING_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val ACCESS_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("access_tls.pem").toFile()
      val key = secretFiles.resolve("access_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MC_ENCRYPTION_PRIVATE_KEY: PrivateKeyHandle by lazy {
      loadEncryptionPrivateKey(MC_ENCRYPTION_PRIVATE_KEY_NAME)
    }

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    val OPEN_ID_PROVIDERS_CONFIG_JSON_FILE: File =
      LOCAL_K8S_PATH.resolve("open_id_providers_config.json").toFile()
    val OPEN_ID_PROVIDERS_TINK_FILE: File =
      SECRET_FILES_PATH.resolve("open_id_provider.tink").toFile()

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

    fun createAccessPrincipal(
      measurementConsumer: String,
      accessChannel: Channel,
      issuer: String,
    ): Principal {
      val rolesStub = RolesGrpc.newBlockingStub(accessChannel)
      val mcUserRoleKey = RoleKey("mcUser")
      val mcResourceType = "halo.wfanet.org/MeasurementConsumer"
      val mcUserRole =
        try {
          rolesStub.getRole(getRoleRequest { name = mcUserRoleKey.toName() })
        } catch (e: Exception) {
          rolesStub.createRole(
            createRoleRequest {
              roleId = mcUserRoleKey.roleId
              role = role {
                resourceTypes += mcResourceType
                permissions +=
                  PERMISSIONS_CONFIG.permissionsMap
                    .filterValues { it.protectedResourceTypesList.contains(mcResourceType) }
                    .keys
                    .map { PermissionKey(it).toName() }
              }
            }
          )
        }

      val principalsStub = PrincipalsGrpc.newBlockingStub(accessChannel)
      val principalKey = PrincipalKey("mc-user")
      val principal =
        try {
          principalsStub.getPrincipal(getPrincipalRequest { name = principalKey.toName() })
        } catch (e: Exception) {
          principalsStub.createPrincipal(
            createPrincipalRequest {
              principalId = principalKey.principalId
              this.principal = principal {
                user =
                  PrincipalKt.oAuthUser {
                    this.issuer = issuer
                    subject = "mc-user@example.com"
                  }
              }
            }
          )
        }

      val policiesStub = PoliciesGrpc.newBlockingStub(accessChannel)
      val policyKey = PolicyKey("test-mc-policy")
      try {
        policiesStub.getPolicy(getPolicyRequest { name = policyKey.toName() })
      } catch (e: Exception) {
        policiesStub.createPolicy(
          createPolicyRequest {
            policyId = policyKey.policyId
            policy = policy {
              protectedResource = measurementConsumer
              bindings +=
                PolicyKt.binding {
                  this.role = mcUserRole.name
                  members += principal.name
                }
            }
          }
        )
      }

      return principal
    }
  }
}
