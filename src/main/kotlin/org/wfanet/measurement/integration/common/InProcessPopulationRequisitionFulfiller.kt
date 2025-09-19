// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

class InProcessPopulationRequisitionFulfiller(
  private val pdpData: DataProviderData,
  private val resourceName: String,
  private val kingdomPublicApiChannel: Channel,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  daemonContext: CoroutineContext = Dispatchers.Default,
) : TestRule {
  private val daemonScope = CoroutineScope(daemonContext)
  private lateinit var populationRequisitionFulfillerJob: Job

  private val modelRolloutsClient by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(resourceName)
  }

  private val modelReleasesClient by lazy {
    ModelReleasesGrpcKt.ModelReleasesCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(resourceName)
  }

  private val populationsClient by lazy {
    PopulationsGrpcKt.PopulationsCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(resourceName)
  }

  private val certificatesClient by lazy {
    CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(resourceName)
  }

  private val requisitionsClient by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(resourceName)
  }

  fun start() {
    populationRequisitionFulfillerJob =
      daemonScope.launch(
        CoroutineName("Population Requisition Fulfiller Daemon") +
          CoroutineExceptionHandler { _, e ->
            logger.log(Level.SEVERE, e) { "Error in Population Requisition Fulfiller Daemon" }
          }
      ) {
        val populationRequisitionFulfiller =
          PopulationRequisitionFulfiller(
            pdpData,
            certificatesClient,
            requisitionsClient,
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
            trustedCertificates,
            modelRolloutsClient,
            modelReleasesClient,
            populationsClient,
            TestEvent.getDescriptor(),
          )
        populationRequisitionFulfiller.run()
      }
  }

  suspend fun stop() {
    populationRequisitionFulfillerJob.cancelAndJoin()
  }

  override fun apply(statement: Statement, description: Description): Statement {
    return statement
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
