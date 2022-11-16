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
import io.kubernetes.client.custom.V1Patch
import io.kubernetes.client.extended.kubectl.Kubectl
import io.kubernetes.client.openapi.ApiCallback
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.JSON
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1ConfigMap
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodList
import io.kubernetes.client.openapi.models.V1ServiceAccount
import io.kubernetes.client.openapi.models.V1Status
import io.kubernetes.client.util.Namespaces
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Yaml
import java.io.File
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
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

class KubernetesClient(
  val apiClient: ApiClient = Configuration.getDefaultApiClient(),
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
) {
  private val coreApi = CoreV1Api(apiClient)
  private val appsApi = AppsV1Api(apiClient)

  /** Restarts a [V1Deployment] by adding an annotation. */
  suspend fun restartDeployment(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT
  ): V1Deployment {
    return appsApi.restartDeployment(name, namespace)
  }

  /** Lists [V1Pod]s for [deployment] based on its match labels. */
  suspend fun listPodsByMatchLabels(deployment: V1Deployment): V1PodList {
    return coreApi.listPodsByMatchLabels(deployment)
  }

  /**
   * Updates a [V1ConfigMap].
   *
   * @param name name of the [V1ConfigMap]
   * @param namespace namespace of the [V1ConfigMap]
   * @param key key of the `data` entry to put
   * @param value value of the `data` entry to put
   * @return the updated [V1ConfigMap]
   */
  suspend fun updateConfigMap(
    name: String,
    key: String,
    value: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT
  ): V1ConfigMap {
    return coreApi.updateConfigMap(name, key, value, namespace)
  }

  private inline fun <reified T : KubernetesObject> watch(
    call: okhttp3.Call
  ): Flow<Watch.Response<T>> {
    return watch(call, object : TypeToken<Watch.Response<T>>() {})
  }

  private fun <T : KubernetesObject> watch(
    call: okhttp3.Call,
    typeToken: TypeToken<Watch.Response<T>>
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

  /** Suspends until the [V1Deployment] has all of its replicas ready. */
  suspend fun waitUntilDeploymentReady(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
    timeout: Duration
  ): V1Deployment {
    return watch<V1Deployment>(
        appsApi.listNamespacedDeploymentCall(
          namespace,
          null,
          null,
          null,
          "metadata.name=$name",
          null,
          null,
          null,
          null,
          timeout.seconds.toInt(),
          true,
          null
        )
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
      .first { it.ready }
  }

  /** Suspends until [deployment] has all of its replicas ready. */
  suspend fun waitUntilDeploymentReady(deployment: V1Deployment, timeout: Duration): V1Deployment {
    return waitUntilDeploymentReady(
      requireNotNull(deployment.metadata?.name),
      requireNotNull(deployment.metadata?.namespace),
      timeout
    )
  }

  /** Suspends until the [V1ServiceAccount] exists. */
  suspend fun waitForServiceAccount(
    name: String,
    namespace: String = Namespaces.NAMESPACE_DEFAULT,
    timeout: Duration
  ): V1ServiceAccount {
    return watch<V1ServiceAccount>(
        coreApi.listNamespacedServiceAccountCall(
          namespace,
          null,
          null,
          null,
          "metadata.name=$name",
          null,
          null,
          null,
          null,
          timeout.seconds.toInt(),
          true,
          null
        )
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
  fun kubectlApply(config: File): Sequence<KubernetesObject> {
    @Suppress("UNCHECKED_CAST") val k8sObjects = Yaml.loadAll(config) as List<KubernetesObject>
    return kubectlApply(k8sObjects)
  }

  @Blocking
  fun kubectlApply(config: String): Sequence<KubernetesObject> {
    @Suppress("UNCHECKED_CAST") val k8sObjects = Yaml.loadAll(config) as List<KubernetesObject>
    return kubectlApply(k8sObjects)
  }

  @Blocking
  fun kubectlApply(k8sObjects: Iterable<KubernetesObject>): Sequence<KubernetesObject> = sequence {
    k8sObjects.map { k8sObject ->
      yield(Kubectl.apply(k8sObject.javaClass).apiClient(apiClient).resource(k8sObject).execute())
    }
  }
}

val ApiException.status: V1Status?
  get() {
    if (responseBody == null) return null

    return Configuration.getDefaultApiClient().json.deserialize(responseBody, V1Status::class.java)
  }

private val V1Deployment.ready: Boolean
  get() {
    val status = checkNotNull(status)
    val numReplicas = status.replicas ?: 0
    val numReadyReplicas = status.readyReplicas ?: 0
    return numReadyReplicas >= 1 && numReadyReplicas == numReplicas
  }

private suspend fun AppsV1Api.restartDeployment(name: String, namespace: String): V1Deployment {
  val patchOps =
    listOf(
      JsonPatchOperation.add(
        "/spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt",
        DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.now())
      )
    )
  return apiCall { callback ->
    patchNamespacedDeploymentAsync(
      name,
      namespace,
      patchOps.toPatch(apiClient.json),
      null,
      null,
      null,
      null,
      null,
      callback
    )
  }
}

private val V1Deployment.labelSelector: String
  get() {
    return checkNotNull(spec?.selector?.matchLabels)
      .map { (key, value) -> "$key=$value" }
      .joinToString(",")
  }

private suspend fun CoreV1Api.listPodsByMatchLabels(deployment: V1Deployment): V1PodList {
  val namespace: String = deployment.metadata?.namespace ?: Namespaces.NAMESPACE_DEFAULT
  return listPods(deployment.labelSelector, namespace)
}

private suspend fun CoreV1Api.listPods(labelSelector: String, namespace: String): V1PodList {
  return apiCall { callback ->
    listNamespacedPodAsync(
      namespace,
      null,
      null,
      null,
      null,
      labelSelector,
      null,
      null,
      null,
      null,
      null,
      callback
    )
  }
}

private suspend fun CoreV1Api.updateConfigMap(
  name: String,
  key: String,
  value: String,
  namespace: String
): V1ConfigMap {
  val patchOps = listOf(JsonPatchOperation.add("/data/$key", value))
  return apiCall { callback ->
    patchNamespacedConfigMapAsync(
      name,
      namespace,
      patchOps.toPatch(apiClient.json),
      null,
      null,
      null,
      null,
      null,
      callback
    )
  }
}

private class DeferredApiCallback<T>
private constructor(private val delegate: CompletableDeferred<T>) :
  ApiCallback<T>, Deferred<T> by delegate {

  constructor() : this(CompletableDeferred())

  override fun onFailure(
    e: ApiException,
    statusCode: Int,
    responseHeaders: Map<String, List<String>>,
  ) {
    delegate.completeExceptionally(e)
  }

  override fun onSuccess(
    result: T,
    statusCode: Int,
    responseHeaders: Map<String, List<String>>,
  ) {
    delegate.complete(result)
  }

  override fun onUploadProgress(bytesWritten: Long, contentLength: Long, done: Boolean) {}

  override fun onDownloadProgress(bytesRead: Long, contentLength: Long, done: Boolean) {}
}

private inline fun <T> apiCallAsync(call: (callback: ApiCallback<T>) -> Unit): Deferred<T> {
  return DeferredApiCallback<T>().also { call(it) }
}

private suspend inline fun <T> apiCall(call: (callback: ApiCallback<T>) -> Unit): T =
  apiCallAsync(call).await()

private fun List<JsonPatchOperation>.toPatch(json: JSON): V1Patch {
  return V1Patch(json.serialize(this))
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
  ERROR
}
