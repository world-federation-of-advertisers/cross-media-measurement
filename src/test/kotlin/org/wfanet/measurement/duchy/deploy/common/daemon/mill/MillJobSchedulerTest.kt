/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.kubernetes.client.openapi.JSON
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobCondition
import io.kubernetes.client.openapi.models.V1JobList
import io.kubernetes.client.openapi.models.V1JobStatus
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1OwnerReference
import io.kubernetes.client.openapi.models.V1PodSpec
import io.kubernetes.client.openapi.models.V1PodTemplate
import io.kubernetes.client.openapi.models.V1PodTemplateSpec
import io.kubernetes.client.util.Namespaces
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.random.Random
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatcher
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.duchy.mill.MillType
import org.wfanet.measurement.duchy.mill.prioritizedStages
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.internal.duchy.claimWorkResponse
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2

@RunWith(JUnit4::class)
class MillJobSchedulerTest {
  private val computationsServiceMock: ComputationsGrpcKt.ComputationsCoroutineImplBase =
    mockService()
  private val k8sClientMock: KubernetesClient = mock {
    onBlocking { getDeployment(any(), any()) } doReturn deployment
    onBlocking { getPodTemplate(eq(LLV2_POD_TEMPLATE_NAME), any()) } doReturn
      liquidLegionsV2PodTemplate
    onBlocking { getPodTemplate(eq(HMSS_POD_TEMPLATE_NAME), any()) } doReturn
      shareShufflePodTemplate
  }

  @JvmField @Rule val serverRule = GrpcTestServerRule { addService(computationsServiceMock) }

  private fun createMillJobScheduler(): MillJobScheduler {
    return MillJobScheduler(
      DUCHY_ID,
      ComputationsGrpcKt.ComputationsCoroutineStub(serverRule.channel),
      DEPLOYMENT_NAME,
      POLLING_DELAY,
      SUCCESSFUL_JOB_HISTORY_LIMIT,
      FAILED_JOB_HISTORY_LIMIT,
      LLV2_POD_TEMPLATE_NAME,
      LLV2_MAX_CONCURRENCY,
      LLV2_WORK_LOCK_DURATION,
      HMSS_POD_TEMPLATE_NAME,
      HMSS_MAX_CONCURRENCY,
      HMSS_WORK_LOCK_DURATION,
      Random(123),
      k8sClientMock,
    )
  }

  @Test
  fun `claims work and creates job when below maximum concurrency`() = runTest {
    whenever(k8sClientMock.listJobs(any(), any())).thenReturn(V1JobList())
    whenever(
        computationsServiceMock.claimWork(
          hasComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)
        )
      )
      .thenReturn(claimWorkResponse { token = computationToken { globalComputationId = "comp-1" } })
    val jobCreated = CompletableDeferred<V1Job>()
    whenever(k8sClientMock.createJob(any())).thenAnswer { invocation ->
      jobCreated.complete(invocation.arguments.first() as V1Job)
      null
    }
    val jobScheduler = createMillJobScheduler()

    backgroundScope.launch { jobScheduler.run() }
    // Wait until createJob has been called.
    val createJobRequest: V1Job = jobCreated.await()

    val claimWorkRequest: ClaimWorkRequest =
      verifyAndCapture(
        computationsServiceMock,
        ComputationsGrpcKt.ComputationsCoroutineImplBase::claimWork,
      )
    assertThat(claimWorkRequest)
      .ignoringFields(ClaimWorkRequest.OWNER_FIELD_NUMBER)
      .isEqualTo(
        claimWorkRequest {
          computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
          prioritizedStages +=
            LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
          lockDuration = LLV2_WORK_LOCK_DURATION.toProtoDuration()
        }
      )
    assertThat(createJobRequest.metadata.labels).containsEntry("mill-type", "LIQUID_LEGIONS_V2")
    assertThat(createJobRequest.metadata.ownerReferences).containsExactly(deploymentOwnerReference)
    assertThat(createJobRequest.spec.template.metadata.labels)
      .containsAtLeastEntriesIn(liquidLegionsV2PodTemplate.template.metadata.labels)
  }

  @Test
  fun `claims work and creates HMSS job when below maximum concurrency`() = runTest {
    whenever(k8sClientMock.listJobs(any(), any())).thenReturn(V1JobList())
    var claimWorkRequest: ClaimWorkRequest? = null
    whenever(
        computationsServiceMock.claimWork(
          hasComputationType(ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE)
        )
      )
      .thenAnswer { invocation ->
        claimWorkRequest = invocation.getArgument(0)
        claimWorkResponse { token = computationToken { globalComputationId = "comp-1" } }
      }
    val jobCreated = CompletableDeferred<V1Job>()
    whenever(k8sClientMock.createJob(any())).thenAnswer { invocation ->
      jobCreated.complete(invocation.arguments.first() as V1Job)
      null
    }
    val jobScheduler = createMillJobScheduler()

    backgroundScope.launch { jobScheduler.run() }
    // Wait until createJob has been called.
    val createJobRequest: V1Job = jobCreated.await()

    assertThat(claimWorkRequest)
      .ignoringFields(ClaimWorkRequest.OWNER_FIELD_NUMBER)
      .isEqualTo(
        claimWorkRequest {
          computationType = ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE
          prioritizedStages += ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE.prioritizedStages
          lockDuration = HMSS_WORK_LOCK_DURATION.toProtoDuration()
        }
      )
    assertThat(createJobRequest.metadata.labels)
      .containsEntry("mill-type", MillType.HONEST_MAJORITY_SHARE_SHUFFLE.name)
    assertThat(createJobRequest.metadata.ownerReferences).containsExactly(deploymentOwnerReference)
    assertThat(createJobRequest.spec.template.metadata.labels)
      .containsAtLeastEntriesIn(shareShufflePodTemplate.template.metadata.labels)
  }

  @Test
  fun `does not claim work when at maximum concurrency`() = runTest {
    val listJobsLatch = CountDownLatch(2)
    whenever(k8sClientMock.listJobs(any(), any())).thenAnswer {
      listJobsLatch.countDown()
      V1JobList().apply {
        repeat(LLV2_MAX_CONCURRENCY) { addItemsItem(buildOwnedJob { status.active = 1 }) }
      }
    }
    val jobScheduler = createMillJobScheduler()

    backgroundScope.launch { jobScheduler.run() }
    // Wait until claimWork has had a chance to be called.
    listJobsLatch.await()

    verify(computationsServiceMock, never()).claimWork(any())
  }

  @Test
  fun `cleans up jobs above history limits`() = runTest {
    val listJobsLatch = CountDownLatch(2)
    val newestCompletionTime = Instant.now()
    whenever(k8sClientMock.listJobs(any(), any())).thenAnswer {
      listJobsLatch.countDown()
      V1JobList().apply {
        repeat(SUCCESSFUL_JOB_HISTORY_LIMIT + 1) { index ->
          addItemsItem(
            buildOwnedJob {
              metadata.name = "successful-job-$index"
              status.addConditionsItem(
                V1JobCondition().apply {
                  type = "Complete"
                  status = "True"
                }
              )
              status.completionTime =
                newestCompletionTime.minusSeconds(index.toLong()).atOffset(ZoneOffset.UTC)
            }
          )
        }
        repeat(FAILED_JOB_HISTORY_LIMIT + 1) { index ->
          addItemsItem(
            buildOwnedJob {
              metadata.name = "failed-job-$index"
              status.addConditionsItem(
                V1JobCondition().apply {
                  type = "Complete"
                  status = "True"
                }
              )
              status.addConditionsItem(
                V1JobCondition().apply {
                  type = "Failed"
                  status = "True"
                }
              )
              status.completionTime =
                newestCompletionTime.minusSeconds(index.toLong()).atOffset(ZoneOffset.UTC)
            }
          )
        }
      }
    }
    val jobScheduler = createMillJobScheduler()

    backgroundScope.launch { jobScheduler.run() }
    // Wait until job cleanup has had a chance to be called.
    listJobsLatch.await()

    val jobNameCaptor = argumentCaptor<String>()
    verify(k8sClientMock, times(2)).deleteJob(jobNameCaptor.capture(), any(), any())
    assertThat(jobNameCaptor.allValues)
      .containsExactly(
        "successful-job-$SUCCESSFUL_JOB_HISTORY_LIMIT",
        "failed-job-$FAILED_JOB_HISTORY_LIMIT",
      )
  }

  private class ComputationTypeMatcher(private val computationType: ComputationType) :
    ArgumentMatcher<ClaimWorkRequest> {
    override fun matches(argument: ClaimWorkRequest): Boolean {
      return argument.computationType == computationType
    }
  }

  companion object {
    init {
      // Ensure JSON for Kubernetes API is initialized.
      JSON()
    }

    private const val DUCHY_ID = "worker1"
    private const val DEPLOYMENT_NAME = "$DUCHY_ID-mill-job-scheduler-deployment"
    private const val LLV2_POD_TEMPLATE_NAME = "$DUCHY_ID-llv2-mill"
    private val LLV2_WORK_LOCK_DURATION = Duration.ofMinutes(10)
    private const val LLV2_MAX_CONCURRENCY = 5
    private const val HMSS_POD_TEMPLATE_NAME = "$DUCHY_ID-hmss-mill"
    private val HMSS_WORK_LOCK_DURATION = Duration.ofMinutes(5)
    private const val HMSS_MAX_CONCURRENCY = 3
    private val POLLING_DELAY = Duration.ofSeconds(2)
    private const val SUCCESSFUL_JOB_HISTORY_LIMIT = 3
    private const val FAILED_JOB_HISTORY_LIMIT = 3

    private val deployment =
      V1Deployment().apply {
        apiVersion = "apps/v1"
        kind = "Deployment"
        metadata =
          V1ObjectMeta().apply {
            name = DEPLOYMENT_NAME
            uid = "$DEPLOYMENT_NAME-uid"
          }
      }
    private val deploymentOwnerReference =
      V1OwnerReference().apply {
        apiVersion = deployment.apiVersion
        kind = deployment.kind
        name = deployment.metadata.name
        uid = deployment.metadata.uid
      }
    private val liquidLegionsV2PodTemplate =
      V1PodTemplate().apply {
        metadata = V1ObjectMeta().apply { name = LLV2_POD_TEMPLATE_NAME }
        template =
          V1PodTemplateSpec().apply {
            metadata = V1ObjectMeta().apply { putLabelsItem("app", "llv2-mill") }
            spec =
              V1PodSpec().apply {
                addContainersItem(V1Container().apply { name = "llv2-mill-container" })
              }
          }
      }
    private val shareShufflePodTemplate =
      V1PodTemplate().apply {
        metadata = V1ObjectMeta().apply { name = HMSS_POD_TEMPLATE_NAME }
        template =
          V1PodTemplateSpec().apply {
            metadata = V1ObjectMeta().apply { putLabelsItem("app", "hmss-mill") }
            spec =
              V1PodSpec().apply {
                addContainersItem(V1Container().apply { name = "hmss-mill-container" })
              }
          }
      }

    private fun buildOwnedJob(fill: V1Job.() -> Unit): V1Job {
      return V1Job().apply {
        metadata =
          V1ObjectMeta().apply {
            namespace = Namespaces.NAMESPACE_DEFAULT
            addOwnerReferencesItem(V1OwnerReference().apply { uid = deployment.metadata.uid })
          }
        status = V1JobStatus()
        fill()
      }
    }

    private fun hasComputationType(computationType: ComputationType) =
      argThat(ComputationTypeMatcher(computationType))
  }
}
