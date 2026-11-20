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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.grpc.StatusException
import java.time.LocalDate
import java.util.LinkedHashMap
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys

/** Resolves managed markers against an invocation-scoped snapshot of public metadata. */
class GrpcManagedDoneObjectResolver(
  private val dataProviderName: String,
  private val uploads: RawImpressionUploadServiceCoroutineStub,
  private val modelLines: RawImpressionUploadModelLineServiceCoroutineStub,
  private val files: RawImpressionUploadFileServiceCoroutineStub,
  private val labelingJobs: VidLabelingJobServiceCoroutineStub,
  private val throttler: Throttler,
) : ManagedDoneObjectResolver {
  private data class ModelOwner(
    val name: String,
    val cmmsModelLine: String,
    val state: RawImpressionUploadModelLine.State,
    val failureReason: RawImpressionUploadModelLine.FailureReason,
  )

  private data class RegisteredInput(val name: String, val uri: String, val date: LocalDate)

  private data class UploadHeader(
    val name: String,
    val state: RawImpressionUpload.State,
    val registrationComplete: Boolean,
    val predecessor: String,
  )

  private val rowsByModelLine = LinkedHashMap<String, List<ModelOwner>>(2, 0.75f, true)
  private val uploadsByName = LinkedHashMap<String, UploadHeader>(32, 0.75f, true)
  private val filesByUpload = LinkedHashMap<String, List<RegisteredInput>>(8, 0.75f, true)
  private val overlappingFiles =
    LinkedHashMap<Pair<String, LocalDate>, Map<String, List<RegisteredInput>>>(4, 0.75f, true)
  private val labelingFilesByUpload =
    LinkedHashMap<Triple<String, String, List<String>>, Boolean>(8, 0.75f, true)

  override suspend fun modelLineFor(eventDate: LocalDate, blobKey: String): String {
    // A zero-output managed marker has no sidecar carrying the full model name. Stream the
    // registered model rows and retain only exact canonical owner matches.
    val matchingNames = mutableSetOf<String>()
    var token = ""
    do {
      val response = callMetadata {
        modelLines.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = "$dataProviderName/rawImpressionUploads/-"
            pageSize = 100
            pageToken = token
          }
        )
      }
      for (row in response.rawImpressionUploadModelLinesList) {
        val owner = ModelOwner(row.name, row.cmmsModelLine, row.state, row.failureReason)
        val canonical =
          LabeledImpressionsBlobKeys.forDone(uploadName(owner), row.cmmsModelLine, eventDate)
        if (blobKey == canonical || blobKey.endsWith("/$canonical"))
          matchingNames += row.cmmsModelLine
      }
      token = response.nextPageToken
    } while (token.isNotEmpty())
    check(matchingNames.size == 1) { "Managed trigger has no unique registered model line" }
    return matchingNames.single()
  }

  override suspend fun resolve(
    modelLine: String,
    eventDate: LocalDate,
    blobKey: String,
  ): ManagedDoneObjectDisposition {
    val rows = readModelLines(modelLine)
    val matching =
      rows.filter {
        LabeledImpressionsBlobKeys.forDone(uploadName(it), modelLine, eventDate)
          .substringAfterLast('/') == blobKey.substringAfterLast('/')
      }
    check(matching.size == 1) { "Managed done object has no unique model-line owner" }
    val owner = matching.single()
    val canonicalKey = LabeledImpressionsBlobKeys.forDone(uploadName(owner), modelLine, eventDate)
    check(blobKey == canonicalKey || blobKey.endsWith("/$canonicalKey")) {
      "Managed done object path does not match its registered date and model line"
    }
    when (owner.state) {
      RawImpressionUploadModelLine.State.FAILED ->
        return if (
          owner.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
        ) {
          ManagedDoneObjectDisposition.OBSOLETE
        } else {
          ManagedDoneObjectDisposition.FINALIZING
        }
      RawImpressionUploadModelLine.State.STATE_UNSPECIFIED,
      RawImpressionUploadModelLine.State.UNRECOGNIZED ->
        error("Managed marker owner has invalid state")
      RawImpressionUploadModelLine.State.CREATED,
      RawImpressionUploadModelLine.State.POOL_ASSIGNING,
      RawImpressionUploadModelLine.State.RANKING,
      RawImpressionUploadModelLine.State.LABELING,
      RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
      RawImpressionUploadModelLine.State.COMPLETED -> {}
    }
    val ownerFiles = readFiles(uploadName(owner)).filter { it.date == eventDate }
    check(ownerFiles.isNotEmpty()) { "Managed done object date is not in its registered input" }
    if (
      owner.state != RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING &&
        owner.state != RawImpressionUploadModelLine.State.COMPLETED
    ) {
      return ManagedDoneObjectDisposition.FINALIZING
    }
    val parent = readUpload(uploadName(owner))
    when (parent.state) {
      RawImpressionUpload.State.CORRECTION_REQUIRED,
      RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT ->
        return ManagedDoneObjectDisposition.OBSOLETE
      RawImpressionUpload.State.CREATED -> return ManagedDoneObjectDisposition.FINALIZING
      RawImpressionUpload.State.ACTIVE,
      RawImpressionUpload.State.FAILED,
      RawImpressionUpload.State.COMPLETED -> {}
      RawImpressionUpload.State.STATE_UNSPECIFIED,
      RawImpressionUpload.State.UNRECOGNIZED -> error("Managed parent has invalid state")
    }
    if (!parent.registrationComplete) return ManagedDoneObjectDisposition.FINALIZING
    val related = readOverlappingFiles(uploadName(owner), eventDate, ownerFiles.map { it.uri })
    val rowsByUpload = rows.associateBy(::uploadName)
    var retained = false
    for (file in ownerFiles) {
      var latest = owner
      for ((upload, candidates) in related) {
        val candidate = rowsByUpload[upload] ?: continue
        if (candidate.name == owner.name) continue
        val inputNames =
          candidates
            .filter { it.uri == file.uri && it.date == eventDate }
            .mapTo(mutableSetOf()) { it.name }
        if (inputNames.isEmpty() || !hasOutputEvent(candidate, inputNames)) continue
        if (descendsFrom(uploadName(candidate), uploadName(latest))) {
          latest = candidate
        } else {
          check(descendsFrom(uploadName(latest), uploadName(candidate))) {
            "Managed output has ambiguous revision owners"
          }
        }
      }
      if (latest.name == owner.name) {
        retained = true
      } else if (
        latest.state != RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING &&
          latest.state != RawImpressionUploadModelLine.State.COMPLETED &&
          latest.failureReason != RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
      ) {
        return ManagedDoneObjectDisposition.FINALIZING
      }
    }
    return if (retained) ManagedDoneObjectDisposition.CURRENT
    else ManagedDoneObjectDisposition.OBSOLETE
  }

  private suspend fun hasOutputEvent(row: ModelOwner, inputNames: Set<String>): Boolean =
    when (row.state) {
      RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
      RawImpressionUploadModelLine.State.COMPLETED -> true
      RawImpressionUploadModelLine.State.FAILED ->
        row.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT ||
          hasLabelingFile(row, inputNames)
      RawImpressionUploadModelLine.State.CREATED,
      RawImpressionUploadModelLine.State.POOL_ASSIGNING,
      RawImpressionUploadModelLine.State.RANKING,
      RawImpressionUploadModelLine.State.LABELING ->
        row.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT ||
          hasLabelingFile(row, inputNames)
      RawImpressionUploadModelLine.State.STATE_UNSPECIFIED,
      RawImpressionUploadModelLine.State.UNRECOGNIZED -> error("Managed revision has invalid state")
    }

  private fun uploadName(row: ModelOwner): String {
    val name =
      row.name.substringBefore("/rawImpressionUploadModelLines/", missingDelimiterValue = "")
    check(name.startsWith("$dataProviderName/rawImpressionUploads/")) {
      "Managed model-line row belongs to a different DataProvider"
    }
    return name
  }

  private fun input(file: RawImpressionUploadFile): RegisteredInput =
    RegisteredInput(
      file.name,
      file.blobUri,
      LocalDate.of(file.eventDate.year, file.eventDate.month, file.eventDate.day),
    )

  private suspend fun readModelLines(modelLine: String): List<ModelOwner> {
    rowsByModelLine[modelLine]?.let {
      return it
    }
    val result = mutableListOf<ModelOwner>()
    var token = ""
    do {
      val response = callMetadata {
        modelLines.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = "$dataProviderName/rawImpressionUploads/-"
            pageSize = 100
            filter = ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = modelLine }
            pageToken = token
          }
        )
      }
      check(response.rawImpressionUploadModelLinesList.all { it.cmmsModelLine == modelLine }) {
        "Managed model-line listing returned a different model line"
      }
      result +=
        response.rawImpressionUploadModelLinesList.map {
          ModelOwner(it.name, it.cmmsModelLine, it.state, it.failureReason)
        }
      token = response.nextPageToken
    } while (token.isNotEmpty())
    remember(rowsByModelLine, modelLine, result, 2)
    return result
  }

  private suspend fun readFiles(upload: String): List<RegisteredInput> {
    filesByUpload[upload]?.let {
      return it
    }
    val result = mutableListOf<RegisteredInput>()
    var token = ""
    do {
      val response = callMetadata {
        files.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = upload
            pageSize = 1000
            pageToken = token
          }
        )
      }
      check(response.rawImpressionUploadFilesList.all { it.name.startsWith("$upload/files/") }) {
        "Managed input listing returned a different upload"
      }
      result += response.rawImpressionUploadFilesList.map(::input)
      token = response.nextPageToken
    } while (token.isNotEmpty())
    remember(filesByUpload, upload, result, 8)
    return result
  }

  private suspend fun readOverlappingFiles(
    upload: String,
    date: LocalDate,
    uris: List<String>,
  ): Map<String, List<RegisteredInput>> {
    val key = upload to date
    overlappingFiles[key]?.let {
      return it
    }
    val result = mutableMapOf<String, MutableList<RegisteredInput>>()
    for (chunk in uris.distinct().chunked(100)) {
      var token = ""
      do {
        val response = callMetadata {
          files.listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = "$dataProviderName/rawImpressionUploads/-"
              pageSize = 1000
              filter = ListRawImpressionUploadFilesRequestKt.filter { blobUriIn += chunk }
              pageToken = token
            }
          )
        }
        check(response.rawImpressionUploadFilesList.all { it.blobUri in chunk }) {
          "Managed overlap listing ignored its URI filter"
        }
        for (file in response.rawImpressionUploadFilesList) {
          val parent = file.name.substringBefore("/files/", missingDelimiterValue = "")
          check(parent.startsWith("$dataProviderName/rawImpressionUploads/")) {
            "Managed overlap listing returned another DataProvider"
          }
          result.getOrPut(parent) { mutableListOf() }.add(input(file))
        }
        token = response.nextPageToken
      } while (token.isNotEmpty())
    }
    remember(overlappingFiles, key, result, 4)
    return result
  }

  private suspend fun readUpload(name: String): UploadHeader {
    uploadsByName[name]?.let {
      return it
    }
    val value = callMetadata {
      uploads.getRawImpressionUpload(getRawImpressionUploadRequest { this.name = name })
    }
    check(value.name == name) { "Managed upload lookup returned another revision" }
    val result =
      UploadHeader(
        value.name,
        value.state,
        value.registrationComplete,
        value.replacesRawImpressionUpload,
      )
    remember(uploadsByName, name, result, 32)
    return result
  }

  private suspend fun descendsFrom(descendant: String, ancestor: String): Boolean {
    var name = descendant
    val visited = mutableSetOf<String>()
    while (name.isNotEmpty()) {
      if (name == ancestor) return true
      check(visited.add(name)) { "Managed upload revisions contain a cycle" }
      name = readUpload(name).predecessor
    }
    return false
  }

  private suspend fun hasLabelingFile(row: ModelOwner, inputNames: Set<String>): Boolean {
    val upload = uploadName(row)
    val key = Triple(upload, row.cmmsModelLine, inputNames.sorted())
    labelingFilesByUpload[key]?.let {
      return it
    }
    var token = ""
    do {
      val response = callMetadata {
        labelingJobs.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = upload
            filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = row.cmmsModelLine }
            pageToken = token
          }
        )
      }
      if (
        response.vidLabelingJobsList.any { job ->
          job.rawImpressionUploadFilesList.any { it in inputNames }
        }
      ) {
        remember(labelingFilesByUpload, key, true, 8)
        return true
      }
      token = response.nextPageToken
    } while (token.isNotEmpty())
    remember(labelingFilesByUpload, key, false, 8)
    return false
  }

  private fun <K, V> remember(cache: LinkedHashMap<K, V>, key: K, value: V, limit: Int) {
    cache[key] = value
    if (cache.size > limit) cache.remove(cache.keys.first())
  }

  private suspend fun <T> callMetadata(block: suspend () -> T): T =
    try {
      throttler.onReady(block)
    } catch (e: StatusException) {
      throw Exception("Managed output ownership lookup failed", e)
    }
}
