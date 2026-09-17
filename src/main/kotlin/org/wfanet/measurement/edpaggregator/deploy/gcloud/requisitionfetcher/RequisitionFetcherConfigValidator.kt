/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.protobuf.util.Durations
import java.time.Duration
import org.wfanet.measurement.config.edpaggregator.DataProviderRequisitionConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionWorkItemDispatchConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.StoragePathPrefixes
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerParamsValidator

/** Validates RequisitionFetcher direct-dispatch configuration before or during activation. */
object RequisitionFetcherConfigValidator {
  /**
   * Validates [config].
   *
   * Every configured DataProvider must have a complete direct-dispatch block, [controlPlaneTarget]
   * must be set, and [dataWatcherConfig] must prove that the legacy route uses the same queue and
   * that no legacy object-name filter matches a representative object in a direct namespace.
   */
  fun validate(
    config: RequisitionFetcherConfig,
    controlPlaneTarget: String?,
    dataWatcherConfig: DataWatcherConfig? = null,
    storageUriPrefix: (DataProviderRequisitionConfig) -> String = ::storageUriPrefix,
  ) {
    require(config.configsCount > 0) { "RequisitionFetcher config has no data providers." }
    requisitionRefusalDuration(config)
    val namespaces = buildList {
      for (dataProviderConfig in config.configsList) {
        val dispatchConfig =
          requireNotNull(
            dataProviderConfig.workItemDispatch.takeIf { dataProviderConfig.hasWorkItemDispatch() }
          ) {
            "Missing 'work_item_dispatch' for data provider: ${dataProviderConfig.dataProvider}."
          }
        validateDataProvider(dataProviderConfig, dispatchConfig, controlPlaneTarget)
        val root = storageUriPrefix(dataProviderConfig).removeSuffix("/")
        add(
          StoragePathPrefixes.Namespace(
            root,
            dataProviderConfig.storagePathPrefix,
            "legacy DataWatcher path for ${dataProviderConfig.dataProvider}",
          )
        )
        add(
          StoragePathPrefixes.Namespace(
            root,
            dispatchConfig.storagePathPrefix,
            "direct-dispatch path for ${dataProviderConfig.dataProvider}",
          )
        )
        if (dataWatcherConfig != null) {
          requireQueueMatchesLegacyRoute(
            root,
            dataProviderConfig,
            dispatchConfig,
            dataWatcherConfig,
          )
          requireDataWatcherExcludesDirectPath(root, dispatchConfig, dataWatcherConfig)
        }
      }
    }
    StoragePathPrefixes.requireDisjoint(namespaces)
  }

  /** Returns the configured stale-Requisition refusal duration, or the 48-hour default. */
  fun requisitionRefusalDuration(config: RequisitionFetcherConfig): Duration {
    if (!config.hasRequisitionRefusalDuration()) {
      return RequisitionFetcher.DEFAULT_REQUISITION_REFUSAL_DURATION
    }
    val configured = config.requisitionRefusalDuration
    require(Durations.isValid(configured)) { "Invalid 'requisition_refusal_duration'." }
    val duration = Duration.ofSeconds(configured.seconds, configured.nanos.toLong())
    require(!duration.isZero && !duration.isNegative) {
      "'requisition_refusal_duration' must be positive."
    }
    return duration
  }

  private fun validateDataProvider(
    dataProviderConfig: DataProviderRequisitionConfig,
    dispatchConfig: RequisitionWorkItemDispatchConfig,
    controlPlaneTarget: String?,
  ) {
    val dataProvider = dataProviderConfig.dataProvider
    require(dataProvider.isNotBlank()) { "Missing 'data_provider' in config." }
    require(dataProviderConfig.hasRequisitionStorage()) {
      "Missing 'requisition_storage' in config for data provider: $dataProvider."
    }
    require(
      dataProviderConfig.requisitionStorage.hasGcs() ||
        dataProviderConfig.requisitionStorage.hasFileSystem()
    ) {
      "Invalid 'requisition_storage' for data provider: $dataProvider."
    }
    if (dataProviderConfig.requisitionStorage.hasGcs()) {
      require(dataProviderConfig.requisitionStorage.gcs.bucketName.isNotBlank()) {
        "Missing GCS 'bucket_name' for data provider: $dataProvider."
      }
    }
    require(dataProviderConfig.storagePathPrefix.isNotBlank()) {
      "Missing 'storage_path_prefix' for data provider: $dataProvider."
    }
    require(
      dataProviderConfig.storagePathPrefix == dataProviderConfig.storagePathPrefix.trim('/')
    ) {
      "'storage_path_prefix' must not start or end with '/' for data provider: $dataProvider."
    }
    require(dataProviderConfig.edpPrivateKeyPath.isNotBlank()) {
      "Missing 'edp_private_key_path' for data provider: $dataProvider."
    }
    require(dataProviderConfig.hasCmmsConnection()) {
      "Missing 'cmms_connection' for data provider: $dataProvider."
    }
    requireTls(
      dataProviderConfig.cmmsConnection.certFilePath,
      dataProviderConfig.cmmsConnection.privateKeyFilePath,
      dataProviderConfig.cmmsConnection.certCollectionFilePath,
      "cmms_connection for data provider: $dataProvider",
    )
    require(dataProviderConfig.hasRequisitionMetadataStorageConnection()) {
      "Missing 'requisition_metadata_storage_connection' for data provider: $dataProvider."
    }
    requireTls(
      dataProviderConfig.requisitionMetadataStorageConnection.certFilePath,
      dataProviderConfig.requisitionMetadataStorageConnection.privateKeyFilePath,
      dataProviderConfig.requisitionMetadataStorageConnection.certCollectionFilePath,
      "requisition_metadata_storage_connection for data provider: $dataProvider",
    )

    require(!controlPlaneTarget.isNullOrBlank()) {
      "Missing Secure Computation control-plane target for direct dispatch."
    }
    require(dispatchConfig.storagePathPrefix.isNotBlank()) {
      "Missing 'storage_path_prefix' in direct-dispatch config for data provider: $dataProvider."
    }
    require(dispatchConfig.storagePathPrefix == dispatchConfig.storagePathPrefix.trim('/')) {
      "Direct-dispatch 'storage_path_prefix' must not start or end with '/' for data provider: " +
        dataProvider
    }
    require(
      !StoragePathPrefixes.overlap(
        dispatchConfig.storagePathPrefix,
        dataProviderConfig.storagePathPrefix,
      )
    ) {
      "Direct-dispatch storage_path_prefix overlaps the legacy storage_path_prefix for data " +
        "provider: $dataProvider."
    }
    require(dispatchConfig.queue.isNotBlank()) {
      "Missing 'queue' in direct-dispatch config for data provider: $dataProvider."
    }
    require(dispatchConfig.hasResultsFulfillerParams()) {
      "Missing 'results_fulfiller_params' in direct-dispatch config for data provider: $dataProvider."
    }
    ResultsFulfillerParamsValidator.validate(dispatchConfig.resultsFulfillerParams, dataProvider)
    require(dispatchConfig.hasControlPlaneConnection()) {
      "Missing 'control_plane_connection' in direct-dispatch config for data provider: $dataProvider."
    }
    requireTls(
      dispatchConfig.controlPlaneConnection.certFilePath,
      dispatchConfig.controlPlaneConnection.privateKeyFilePath,
      dispatchConfig.controlPlaneConnection.certCollectionFilePath,
      "direct-dispatch control_plane_connection for data provider: $dataProvider",
    )
  }

  private fun requireTls(cert: String, key: String, roots: String, owner: String) {
    require(cert.isNotBlank()) { "Missing 'cert_file_path' in $owner." }
    require(key.isNotBlank()) { "Missing 'private_key_file_path' in $owner." }
    require(roots.isNotBlank()) { "Missing 'cert_collection_file_path' in $owner." }
  }

  private fun requireDataWatcherExcludesDirectPath(
    storageUriPrefix: String,
    dispatchConfig: RequisitionWorkItemDispatchConfig,
    dataWatcherConfig: DataWatcherConfig,
  ) {
    val directPrefix = "$storageUriPrefix/${dispatchConfig.storagePathPrefix.trim('/')}"
    val representativePaths =
      listOf(directPrefix, "$directPrefix/", "$directPrefix/$REPRESENTATIVE_GROUP_ID")
    for (watchedPath in dataWatcherConfig.watchedPathsList) {
      val regex = watchedPath.sourcePathRegex.toRegex()
      require(representativePaths.none(regex::matches)) {
        "DataWatcher path '${watchedPath.identifier}' matches direct-dispatch prefix " +
          "'$directPrefix'."
      }
    }
  }

  private fun requireQueueMatchesLegacyRoute(
    storageUriPrefix: String,
    dataProviderConfig: DataProviderRequisitionConfig,
    dispatchConfig: RequisitionWorkItemDispatchConfig,
    dataWatcherConfig: DataWatcherConfig,
  ) {
    val legacyPrefix = "$storageUriPrefix/${dataProviderConfig.storagePathPrefix.trim('/')}"
    val representativePaths =
      listOf(legacyPrefix, "$legacyPrefix/", "$legacyPrefix/$REPRESENTATIVE_GROUP_ID")
    val deployedQueues =
      dataWatcherConfig.watchedPathsList
        .filter { watchedPath ->
          watchedPath.hasControlPlaneQueueSink() &&
            representativePaths.any(watchedPath.sourcePathRegex.toRegex()::matches)
        }
        .map { it.controlPlaneQueueSink.queue }
        .distinct()
    require(deployedQueues.size == 1) {
      "Expected exactly one deployed ResultsFulfiller queue for legacy path '$legacyPrefix', " +
        "found: ${deployedQueues.ifEmpty { listOf("none") }.joinToString()}."
    }
    val deployedQueue = deployedQueues.single()
    require(dispatchConfig.queue == deployedQueue) {
      "Direct-dispatch queue '${dispatchConfig.queue}' does not match deployed " +
        "ResultsFulfiller queue '$deployedQueue' for data provider: " +
        "${dataProviderConfig.dataProvider}."
    }
  }

  private fun storageUriPrefix(config: DataProviderRequisitionConfig): String {
    return when (config.requisitionStorage.storageCase) {
      StorageParams.StorageCase.GCS -> "gs://${config.requisitionStorage.gcs.bucketName}"
      StorageParams.StorageCase.FILE_SYSTEM -> "file://"
      StorageParams.StorageCase.STORAGE_NOT_SET ->
        throw IllegalArgumentException("Requisition storage is not configured")
    }
  }

  private const val REPRESENTATIVE_GROUP_ID = "00000000-0000-0000-0000-000000000000"
}
