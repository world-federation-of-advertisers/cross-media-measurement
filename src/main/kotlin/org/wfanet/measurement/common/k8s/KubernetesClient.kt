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

package org.wfanet.measurement.common.k8s

import com.google.gson.reflect.TypeToken
import io.kubernetes.client.common.KubernetesObject
import io.kubernetes.client.extended.kubectl.Kubectl
import io.kubernetes.client.openapi.ApiCallback
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.ApiResponse
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.JSON
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobList
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodList
import io.kubernetes.client.openapi.models.V1PodTemplate
import io.kubernetes.client.openapi.models.V1PodTemplateSpec
import io.kubernetes.client.openapi.models.V1ReplicaSet
import io.kubernetes.client.openapi.models.V1ServiceAccount
import io.kubernetes.client.openapi.models.V1Status
import io.kubernetes.client.util.Namespaces
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Yaml
import java.io.File
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.random.Random
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.annotations.Blocking
import org.jetbrains.annotations.BlockingExecutor

/** Client interface for the Kubernetes API. */
interface KubernetesClient {
  val apiClient: ApiClient

  /** Gets a single [V1Deployment] by [name]. */
  suspend fun getDeployment(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
  ): V1Deployment?

  suspend fun getPodTemplate(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
  ): V1PodTemplate?

  /** Gets the [V1ReplicaSet] for the current revision of [deployment]. */
  suspend fun getNewReplicaSet(deployment: V1Deployment): V1ReplicaSet?

  /** Lists Pods for the specified [replicaSet]. */
  suspend fun listPods(replicaSet: V1ReplicaSet): V1PodList

  /** Lists Jobs using the specified [matchLabelsSelector]. */
  suspend fun listJobs(
    matchLabelsSelector: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
  ): V1JobList

  suspend fun createJob(job: V1Job): V1Job

  suspend fun deleteJob(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
    propagationPolicy: PropagationPolicy = PropagationPolicy.BACKGROUND,
  ): V1Status

  /** Suspends until the [V1Deployment] is complete. */
  suspend fun waitUntilDeploymentComplete(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
    timeout: Duration,
  ): V1Deployment

  /** Suspends until the [V1ServiceAccount] exists. */
  suspend fun waitForServiceAccount(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
    timeout: Duration,
  ): V1ServiceAccount

  @Blocking fun kubectlApply(config: File): Sequence<KubernetesObject>

  @Blocking fun kubectlApply(config: String): Sequence<KubernetesObject>

  @Blocking fun kubectlApply(k8sObjects: Iterable<KubernetesObject>): Sequence<KubernetesObject>

  companion object {
    /** Generates a random suffix for a Kubernetes object name. */
    fun generateNameSuffix(random: Random) = Names.generateNameSuffix(random)
  }
}

/** Default implementation of [KubernetesClient]. */
class KubernetesClientImpl(
  override val apiClient: ApiClient = Configuration.getDefaultApiClient(),
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : KubernetesClient {
  private val coreApi = CoreV1Api(apiClient)
  private val appsApi = AppsV1Api(apiClient)
  private val batchApi = BatchV1Api(apiClient)

  override suspend fun getDeployment(name: String, namespace: String): V1Deployment? {
    return awaitData { callback ->
      appsApi.readNamespacedDeployment(name, namespace).executeAsync(callback)
    }
  }

  override suspend fun getPodTemplate(name: String, namespace: String): V1PodTemplate? {
    return awaitData { callback ->
      coreApi.readNamespacedPodTemplate(name, namespace).executeAsync(callback)
    }
  }

  override suspend fun getNewReplicaSet(deployment: V1Deployment): V1ReplicaSet? {
    val namespace: String = deployment.metadata?.namespace ?: Namespaces.NAMESPACE_DEFAULT
    val labelSelector = deployment.labelSelector
    val revision = deployment.metadata?.annotations?.get(REVISION_ANNOTATION) ?: return null

    return awaitData { callback ->
        appsApi
          .listNamespacedReplicaSet(namespace)
          .labelSelector(labelSelector)
          .executeAsync(callback)
      }
      .items
      .find { it.metadata?.annotations?.get(REVISION_ANNOTATION) == revision }
  }

  override suspend fun listPods(replicaSet: V1ReplicaSet): V1PodList {
    val namespace: String = replicaSet.metadata?.namespace ?: Namespaces.NAMESPACE_DEFAULT
    val labelSelector: V1LabelSelector = checkNotNull(replicaSet.spec).selector

    return awaitData { callback ->
      coreApi
        .listNamespacedPod(namespace)
        .labelSelector(labelSelector.matchLabelsSelector)
        .executeAsync(callback)
    }
  }

  override suspend fun listJobs(matchLabelsSelector: String, namespace: String): V1JobList {
    return awaitData { callback ->
      batchApi
        .listNamespacedJob(namespace)
        .labelSelector(matchLabelsSelector)
        .executeAsync(callback)
    }
  }

  override suspend fun createJob(job: V1Job): V1Job {
    val namespace: String = job.metadata.namespace ?: Namespaces.NAMESPACE_DEFAULT
    return awaitData { callback ->
      batchApi.createNamespacedJob(namespace, job).executeAsync(callback)
    }
  }

  override suspend fun deleteJob(
    name: String,
    namespace: String,
    propagationPolicy: PropagationPolicy,
  ): V1Status {
    return awaitData { callback ->
      batchApi
        .deleteNamespacedJob(name, namespace)
        .propagationPolicy(propagationPolicy.value)
        .executeAsync(callback)
    }
  }

  private inline fun <reified T : KubernetesObject> watch(
    call: okhttp3.Call
  ): Flow<Watch.Response<T>> {
    return watch(call, object : TypeToken<Watch.Response<T>>() {})
  }

  private fun <T : KubernetesObject> watch(
    call: okhttp3.Call,
    typeToken: TypeToken<Watch.Response<T>>,
  ): Flow<Watch.Response<T>> {
    return channelFlow {
        val closed = AtomicBoolean(false)
        val watch = Watch.createWatch<T>(apiClient, call, typeToken.type)
        launch {
          fun isActive(): Boolean = coroutineContext.isActive && !closed.get()

          try {
            while (isActive() && watch.hasNext()) {
              send(watch.next())
            }
          } catch (e: RuntimeException) {
            if (isActive()) {
              throw e
            }
          }
        }
        awaitClose {
          closed.set(true)
          watch.close()
        }
      }
      .buffer(Channel.RENDEZVOUS)
      .flowOn(coroutineContext)
  }

  override suspend fun waitUntilDeploymentComplete(
    name: String,
    namespace: String,
    timeout: Duration,
  ): V1Deployment {
    return watch<V1Deployment>(
        appsApi
          .listNamespacedDeployment(namespace)
          .fieldSelector("metadata.name=$name")
          .timeoutSeconds(timeout.seconds.toInt())
          .watch(true)
          .buildCall(null)
      )
      .filter { response: Watch.Response<V1Deployment> ->
        when (WatchEventType.valueOf(response.type)) {
          WatchEventType.ADDED,
          WatchEventType.MODIFIED -> true
          WatchEventType.DELETED -> false
          WatchEventType.BOOKMARK,
          WatchEventType.ERROR -> error("Unexpected WatchEventType ${response.type}")
        }
      }
      .map { it.`object` }
      .first { it.complete }
  }

  override suspend fun waitForServiceAccount(
    name: String,
    namespace: String,
    timeout: Duration,
  ): V1ServiceAccount {
    return watch<V1ServiceAccount>(
        coreApi
          .listNamespacedServiceAccount(namespace)
          .fieldSelector("metadata.name=$name")
          .timeoutSeconds(timeout.seconds.toInt())
          .watch(true)
          .buildCall(null)
      )
      .filter {
        when (WatchEventType.valueOf(it.type)) {
          WatchEventType.ADDED,
          WatchEventType.MODIFIED -> true
          WatchEventType.DELETED -> false
          WatchEventType.BOOKMARK,
          WatchEventType.ERROR -> error("Unexpected WatchEventType ${it.type}")
        }
      }
      .map { it.`object` }
      .first()
  }

  @Blocking
  override fun kubectlApply(config: File): Sequence<KubernetesObject> {
    @Suppress("UNCHECKED_CAST") val k8sObjects = Yaml.loadAll(config) as List<KubernetesObject>
    return kubectlApply(k8sObjects)
  }

  @Blocking
  override fun kubectlApply(config: String): Sequence<KubernetesObject> {
    @Suppress("UNCHECKED_CAST") val k8sObjects = Yaml.loadAll(config) as List<KubernetesObject>
    return kubectlApply(k8sObjects)
  }

  @Blocking
  override fun kubectlApply(k8sObjects: Iterable<KubernetesObject>): Sequence<KubernetesObject> =
    sequence {
      k8sObjects.map { k8sObject ->
        // TODO(kubernetes-client/java#3076): Remove when fixed.
        if (k8sObject is V1Pod) {
          val podSpec = k8sObject.spec
          if (podSpec.overhead != null && podSpec.overhead.isEmpty()) podSpec.overhead(null)
        }

        yield(Kubectl.apply(k8sObject.javaClass).apiClient(apiClient).resource(k8sObject).execute())
      }
    }

  companion object {
    private const val REVISION_ANNOTATION = "deployment.kubernetes.io/revision"
  }
}

val ApiException.status: V1Status?
  get() {
    if (responseBody == null) return null

    return JSON.deserialize<V1Status>(responseBody, V1Status::class.java)
  }

/**
 * Whether the Deployment is complete.
 *
 * See https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#complete-deployment
 */
private val V1Deployment.complete: Boolean
  get() {
    val conditions = status?.conditions ?: return false
    val progressingCondition = conditions.find { it.type == "Progressing" } ?: return false
    return progressingCondition.status == "True" &&
      progressingCondition.reason == "NewReplicaSetAvailable"
  }

private val V1Deployment.labelSelector: String
  get() = checkNotNull(spec?.selector).matchLabelsSelector

val V1LabelSelector.matchLabelsSelector: String
  get() {
    return matchLabels.map { (key, value) -> "$key=$value" }.joinToString(",")
  }

fun V1PodTemplateSpec.clone(): V1PodTemplateSpec {
  return V1PodTemplateSpec.fromJson(toJson())
}

val V1Job.failed: Boolean
  get() {
    val conditions = status.conditions ?: emptyList()
    return conditions.any { it.type == "Failed" && it.status == "True" }
  }

val V1Job.complete: Boolean
  get() {
    val conditions = status.conditions ?: emptyList()
    return conditions.any { it.type == "Complete" && it.status == "True" }
  }

private class CoroutineApiCallback<T>(private val continuation: Continuation<ApiResponse<T>>) :
  ApiCallback<T> {
  override fun onFailure(
    e: ApiException,
    statusCode: Int,
    responseHeaders: Map<String, List<String>>?,
  ) {
    continuation.resumeWithException(e)
  }

  override fun onSuccess(result: T, statusCode: Int, responseHeaders: Map<String, List<String>>) {
    continuation.resume(ApiResponse(statusCode, responseHeaders, result))
  }

  override fun onUploadProgress(bytesWritten: Long, contentLength: Long, done: Boolean) {}

  override fun onDownloadProgress(bytesRead: Long, contentLength: Long, done: Boolean) {}
}

/**
 * Suspends until the [okhttp3.Call] returned by [executeAsync] has a response.
 *
 * @return the [ApiResponse] for the call
 */
private suspend inline fun <T> await(
  crossinline executeAsync: (callback: ApiCallback<T>) -> okhttp3.Call
): ApiResponse<T> = suspendCoroutine { continuation ->
  executeAsync(CoroutineApiCallback(continuation))
}

/**
 * Suspends until the [okhttp3.Call] returned by [executeAsync] has a response.
 *
 * @return the data from the API response
 */
private suspend inline fun <T> awaitData(
  crossinline executeAsync: (callback: ApiCallback<T>) -> okhttp3.Call
): T = await(executeAsync).data

/**
 * Policy for whether and how garbage collection will be performed.
 *
 * See
 * https://kubernetes.io/docs/reference/kubernetes-api/common-parameters/common-parameters/#propagationPolicy
 */
enum class PropagationPolicy(val value: String) {
  ORPHAN("Orphan"),
  BACKGROUND("Background"),
  FOREGROUND("Foreground"),
}

/**
 * Type of Watch event.
 *
 * See https://pkg.go.dev/k8s.io/apimachinery/pkg/watch#EventType
 */
private enum class WatchEventType {
  ADDED,
  MODIFIED,
  DELETED,
  BOOKMARK,
  ERROR,
}
