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

import com.google.common.hash.Hashing
import com.google.rpc.ErrorInfo
import io.grpc.Channel
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.Blocking
import org.junit.Test
import org.wfanet.measurement.access.service.Errors
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.Policy
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.addPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deleteRoleRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.lookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.integration.common.EventQuery
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpec
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.PopulationData
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator

/** Test for correctness of the CMMS on Kubernetes. */
abstract class AbstractCorrectnessTest(private val measurementSystem: MeasurementSystem) {
  private val runId: String
    get() = measurementSystem.runId

  private val testHarness: EventQueryMeasurementConsumerSimulator
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

  @Test
  fun `basic report completes with expected result`() = runBlocking {
    reportingTestHarness.testBasicReport(runId)
  }

  @Test
  fun `population measurement completes with expected result`() = runBlocking {
    testHarness.testPopulation(
      "$runId-population",
      measurementSystem.getPopulationData(),
      measurementSystem.modelLineName,
      "person.gender == ${Person.Gender.FEMALE_VALUE} && " +
        "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
      TestEvent.getDescriptor(),
    )
  }

  abstract class MeasurementSystem() {
    abstract val runId: String
    abstract val testHarness: EventQueryMeasurementConsumerSimulator
    abstract val reportingTestHarness: ReportingUserSimulator

    /** Resource name of the Population Data Provider (PDP). */
    protected abstract val populationDataProviderName: String

    /**
     * Synthetic population spec.
     *
     * This must match the spec used by EDP simulators.
     */
    abstract val syntheticPopulationSpec: SyntheticPopulationSpec

    /**
     * Synthetic event group specs.
     *
     * These must match the specs used by the EDP simulators, in order.
     */
    abstract val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec>

    /** [ModelLine] Resource Name of an active ModelLine. */
    abstract val modelLineName: String

    fun buildEventQuery(dataProviderNames: Iterable<String>) =
      EventQuery(syntheticPopulationSpec, syntheticEventGroupSpecs, dataProviderNames)

    fun getPopulationData(): PopulationData {
      val populationSpec = syntheticPopulationSpec.toPopulationSpec(TestEvent.getDescriptor())
      return PopulationData(populationDataProviderName, populationSpec)
    }

    /**
     * Returns the minimum active start date of a [ModelLine] that will cover
     * [syntheticEventGroupSpecs].
     */
    protected fun getMinModelLineStartDate(): LocalDate {
      var startDate = LocalDate.MAX
      for (eventGroupSpec in syntheticEventGroupSpecs) {
        for (dateSpec in eventGroupSpec.dateSpecsList) {
          startDate = minOf(startDate, dateSpec.dateRange.start.toLocalDate())
        }
      }

      return startDate
    }

    /**
     * Ensures that a [Population] resource exists in the CMMS public API for the value returned by
     * [getPopulationData].
     */
    @Blocking
    protected fun ensurePopulation(kingdomPublicApiChannel: Channel): Population {
      val populationData = getPopulationData()
      val populationsStub = PopulationsGrpc.newBlockingStub(kingdomPublicApiChannel)

      return try {
        populationsStub.createPopulation(
          createPopulationRequest {
            parent = populationData.populationDataProviderName
            population = population { populationSpec = populationData.populationSpec }
            // This is relying on protobuf serialization which is known not to be deterministic
            // across builds. That said, it's sufficient to avoid creating too many duplicate
            // Populations.
            @OptIn(ExperimentalStdlibApi::class) // For `HexFormat`.
            requestId =
              Hashing.murmur3_128()
                .hashBytes(populationData.populationSpec.toByteArray())
                .asBytes()
                .toHexString(HexFormat.Default)
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating population", e)
      }
    }
  }

  companion object {
    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"
    const val PDP_DISPLAY_NAME = "pdp1"
    const val MP_DISPLAY_NAME = "mp1"

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

    val LOCAL_K8S_PATH: Path = Paths.get("src", "main", "k8s", "local")
    val OPEN_ID_PROVIDERS_CONFIG_JSON_FILE: File =
      LOCAL_K8S_PATH.resolve("open_id_providers_config.json").toFile()
    val OPEN_ID_PROVIDERS_TINK_FILE: File =
      SECRET_FILES_PATH.resolve("open_id_provider.tink").toFile()

    val PDP_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("${PDP_DISPLAY_NAME}_tls.pem").toFile()
      val key = secretFiles.resolve("${PDP_DISPLAY_NAME}_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MP_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("${MP_DISPLAY_NAME}_tls.pem").toFile()
      val key = secretFiles.resolve("${MP_DISPLAY_NAME}_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

    /**
     * Ensures that [Principal] exists with specific field values along with corresponding [Policy].
     * Will create [Role], [Policy], and [Principal] as needed.
     *
     * @return [Principal]
     */
    fun createAccessPrincipal(
      measurementConsumer: String,
      accessChannel: Channel,
      issuer: String,
    ): Principal {
      val rolesStub = RolesGrpc.newBlockingStub(accessChannel)

      val mcUserRoleKey = RoleKey("mcUser")
      val mcResourceType = "halo.wfanet.org/MeasurementConsumer"

      val permissions =
        PERMISSIONS_CONFIG.permissionsMap
          .filterValues { it.protectedResourceTypesList.contains(mcResourceType) }
          .keys
          .map { PermissionKey(it).toName() }

      var mcUserRole: Role = Role.getDefaultInstance()
      try {
        val role = rolesStub.getRole(getRoleRequest { name = mcUserRoleKey.toName() })

        if (
          role.resourceTypesList.contains(mcResourceType) &&
            role.permissionsList.containsAll(permissions)
        ) {
          mcUserRole = role
        } else {
          rolesStub.deleteRole(deleteRoleRequest { name = role.name })
        }
      } catch (e: StatusRuntimeException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
          // Role not found so will create it
        } else {
          throw e
        }
      }

      if (mcUserRole.name.isEmpty()) {
        val createRoleRequest = createRoleRequest {
          roleId = mcUserRoleKey.roleId
          role = role {
            resourceTypes += mcResourceType
            this.permissions +=
              PERMISSIONS_CONFIG.permissionsMap
                .filterValues { it.protectedResourceTypesList.contains(mcResourceType) }
                .keys
                .map { PermissionKey(it).toName() }
          }
        }

        mcUserRole = rolesStub.createRole(createRoleRequest)
      }

      val principalsStub = PrincipalsGrpc.newBlockingStub(accessChannel)
      val oauthUser =
        PrincipalKt.oAuthUser {
          this.issuer = issuer
          subject = "mc-user@example.com"
        }

      val principal: Principal =
        try {
          principalsStub.lookupPrincipal(lookupPrincipalRequest { user = oauthUser })
        } catch (e: StatusRuntimeException) {
          val errorInfo: ErrorInfo = e.errorInfo ?: throw e

          if (errorInfo.reason == Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER.name) {
            principalsStub.createPrincipal(
              createPrincipalRequest {
                principalId = "mc-user"
                this.principal = principal { user = oauthUser }
              }
            )
          } else {
            throw e
          }
        }

      val policiesStub = PoliciesGrpc.newBlockingStub(accessChannel)
      try {
        val policy =
          policiesStub.lookupPolicy(lookupPolicyRequest { protectedResource = measurementConsumer })
        for (binding in policy.bindingsList) {
          if (binding.role == mcUserRole.name && binding.membersList.contains(principal.name)) {
            return principal
          }
        }

        policiesStub.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            name = policy.name
            role = mcUserRole.name
            members += principal.name
          }
        )
      } catch (e: StatusRuntimeException) {
        if (e.errorInfo == null) {
          throw e
        }

        if (e.errorInfo!!.reason == Errors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE.name) {
          policiesStub.createPolicy(
            createPolicyRequest {
              policyId = "test-mc-policy"
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
        } else {
          throw e
        }
      }

      return principal
    }
  }
}
