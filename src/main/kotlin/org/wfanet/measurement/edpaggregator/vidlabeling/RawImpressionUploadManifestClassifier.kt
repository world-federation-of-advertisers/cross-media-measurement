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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.type.Date
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt as InternalCandidateKt

/** Reconstructs effective raw-upload manifests and classifies complete revisions. */
class RawImpressionUploadManifestClassifier {
  /** Classification of a complete raw-upload revision. */
  enum class Classification {
    NEW,
    NO_OP,
    APPEND,
    EDITED,
    REMOVED,
    MIXED,
  }

  /** One immutable object version in a raw-upload manifest. */
  data class File(
    val blobUri: String,
    val blobGeneration: Long,
    val eventDate: Date = Date.getDefaultInstance(),
  )

  /** One persisted raw-upload revision and its registered files. */
  data class Revision(
    val rawImpressionUpload: String,
    val doneBlobUri: String,
    val doneBlobGeneration: Long,
    val doneBlobCreateTime: Instant?,
    val createTime: Instant,
    val replacesRawImpressionUpload: String = "",
    val uploadHealingOperation: String = "",
    val registrationComplete: Boolean = true,
    val failed: Boolean = false,
    val quarantined: Boolean = false,
    val manifestBoundary: Boolean = false,
    val files: List<File>,
  )

  /** One effective manifest entry and the revision that produced its visible output. */
  data class ManifestEntry(val file: File, val outputSourceRawImpressionUpload: String)

  /** One difference between the effective prior and complete current manifests. */
  data class Difference(
    val blobUri: String,
    val prior: ManifestEntry?,
    val current: ManifestEntry?,
  )

  /** Classification result with stable digests and ordered differences. */
  data class Result(
    val classification: Classification,
    val priorManifest: Map<String, ManifestEntry>,
    val currentManifest: Map<String, ManifestEntry>,
    val differences: List<Difference>,
    val priorManifestDigest: ByteString,
    val currentManifestDigest: ByteString,
  )

  /** Classifies [currentRawImpressionUpload] against its effective prior manifest. */
  fun classify(currentRawImpressionUpload: String, revisions: Collection<Revision>): Result {
    val revisionsByName = revisions.associateByUniqueName()
    val current =
      requireNotNull(revisionsByName[currentRawImpressionUpload]) {
        "Current RawImpressionUpload $currentRawImpressionUpload is missing"
      }
    val currentManifest = current.toManifest()
    val hasPriorRevision = predecessorOf(current, revisionsByName) != null
    val priorManifest = reconstructPriorManifest(current, revisionsByName)
    val differences =
      (priorManifest.keys + currentManifest.keys).sorted().mapNotNull { blobUri ->
        val prior = priorManifest[blobUri]
        val currentEntry = currentManifest[blobUri]
        if (
          prior != null &&
            currentEntry != null &&
            prior.file.blobGeneration == currentEntry.file.blobGeneration &&
            !eventDateChanged(prior.file, currentEntry.file)
        ) {
          null
        } else {
          Difference(blobUri, prior, currentEntry)
        }
      }
    return Result(
      classification = classify(hasPriorRevision, differences),
      priorManifest = priorManifest,
      currentManifest = currentManifest,
      differences = differences,
      priorManifestDigest = digestManifest(priorManifest),
      currentManifestDigest = digestManifest(currentManifest),
    )
  }

  /** Converts [result] to the immutable comparison persisted with a correction candidate. */
  fun snapshot(
    result: Result,
    ownerResourceId: (String) -> String,
  ): InternalCandidate.ManifestComparison =
    InternalCandidateKt.manifestComparison {
      priorManifest +=
        result.priorManifest.values.sortedBy { it.file.blobUri }.map { it.toProto(ownerResourceId) }
      currentManifest +=
        result.currentManifest.values
          .sortedBy { it.file.blobUri }
          .map { it.toProto(ownerResourceId) }
      differences +=
        result.differences.map { difference ->
          InternalCandidateKt.manifestDifference {
            type =
              when {
                difference.prior == null -> InternalCandidate.ManifestDifference.Type.TYPE_ADDED
                difference.current == null -> InternalCandidate.ManifestDifference.Type.TYPE_REMOVED
                eventDateChanged(difference.prior.file, difference.current.file) ->
                  InternalCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED
                else -> InternalCandidate.ManifestDifference.Type.TYPE_EDITED
              }
            val prior = difference.prior
            if (prior != null) this.prior = prior.toProto(ownerResourceId)
            val current = difference.current
            if (current != null) this.current = current.toProto(ownerResourceId)
          }
        }
    }

  /** Computes the stable digest used to identify an exact raw-object manifest. */
  fun digest(files: Collection<File>): ByteString {
    val manifest = buildMap {
      for (file in files) {
        require(put(file.blobUri, ManifestEntry(file, "")) == null) {
          "Duplicate blob URI ${file.blobUri}"
        }
      }
    }
    return digestManifest(manifest)
  }

  /** Reconstructs the effective manifest immediately before [revision]. */
  fun reconstructPriorManifest(
    revision: Revision,
    revisions: Collection<Revision>,
  ): Map<String, ManifestEntry> =
    reconstructPriorManifest(revision, revisions.associateByUniqueName())

  /** Reconstructs the effective manifest represented by a persisted revision. */
  fun reconstructEffectiveManifest(
    revision: Revision,
    revisions: Collection<Revision>,
  ): Map<String, ManifestEntry> {
    val revisionsByName = revisions.associateByUniqueName()
    val current = revision.toManifest()
    val predecessor = predecessorOf(revision, revisionsByName)
    if (
      revision.quarantined ||
        revision.manifestBoundary ||
        revision.uploadHealingOperation.isNotEmpty() ||
        predecessor == null
    ) {
      return current
    }
    return (reconstructPriorManifest(revision, revisionsByName, stopAtFailedPredecessor = false) +
        current)
      .toSortedMap()
  }

  private fun reconstructPriorManifest(
    revision: Revision,
    revisionsByName: Map<String, Revision>,
    stopAtFailedPredecessor: Boolean = true,
  ): Map<String, ManifestEntry> {
    val result = linkedMapOf<String, ManifestEntry>()
    val visited = mutableSetOf<String>()
    var current = predecessorOf(revision, revisionsByName)
    while (current != null) {
      check(visited.add(current.rawImpressionUpload)) {
        "RawImpressionUpload revision cycle detected at ${current.rawImpressionUpload}"
      }
      if (current.registrationComplete && (!current.quarantined || current.manifestBoundary)) {
        val revisionUris = mutableSetOf<String>()
        for (file in current.files.sortedBy { it.blobUri }) {
          require(revisionUris.add(file.blobUri)) {
            "Duplicate blob URI ${file.blobUri} in ${current.rawImpressionUpload}"
          }
          result.putIfAbsent(file.blobUri, ManifestEntry(file, current.rawImpressionUpload))
        }
      }
      val predecessor = predecessorOf(current, revisionsByName)
      if (
        current.manifestBoundary ||
          current.uploadHealingOperation.isNotEmpty() ||
          predecessor == null ||
          stopAtFailedPredecessor && predecessor.failed
      ) {
        break
      }
      current = predecessor
    }
    return result.toSortedMap()
  }

  private fun predecessorOf(revision: Revision, revisionsByName: Map<String, Revision>): Revision? {
    if (revision.replacesRawImpressionUpload.isNotEmpty()) {
      val predecessor =
        requireNotNull(revisionsByName[revision.replacesRawImpressionUpload]) {
          "RawImpressionUpload ${revision.replacesRawImpressionUpload} is missing"
        }
      require(predecessor.doneBlobUri == revision.doneBlobUri) {
        "RawImpressionUpload revisions use different done objects"
      }
      return predecessor
    }
    return revisionsByName.values
      .asSequence()
      .filter {
        it.rawImpressionUpload != revision.rawImpressionUpload &&
          it.doneBlobUri == revision.doneBlobUri &&
          REVISION_COMPARATOR.compare(it, revision) < 0
      }
      .maxWithOrNull(REVISION_COMPARATOR)
  }

  private fun Revision.toManifest(): Map<String, ManifestEntry> =
    buildMap {
        for (file in files.sortedBy { it.blobUri }) {
          require(put(file.blobUri, ManifestEntry(file, rawImpressionUpload)) == null) {
            "Duplicate blob URI ${file.blobUri} in $rawImpressionUpload"
          }
        }
      }
      .toSortedMap()

  private fun classify(hasPriorRevision: Boolean, differences: List<Difference>): Classification {
    if (!hasPriorRevision) return Classification.NEW
    if (differences.isEmpty()) return Classification.NO_OP
    val hasAdditions = differences.any { it.prior == null }
    val hasEdits =
      differences.any {
        it.prior != null && it.current != null && !eventDateChanged(it.prior.file, it.current.file)
      }
    val hasEventDateChanges =
      differences.any {
        it.prior != null && it.current != null && eventDateChanged(it.prior.file, it.current.file)
      }
    val hasRemovals = differences.any { it.current == null }
    return when {
      hasAdditions && !hasEdits && !hasEventDateChanges && !hasRemovals -> Classification.APPEND
      hasEdits && !hasAdditions && !hasEventDateChanges && !hasRemovals -> Classification.EDITED
      hasRemovals && !hasAdditions && !hasEdits && !hasEventDateChanges -> Classification.REMOVED
      else -> Classification.MIXED
    }
  }

  private fun eventDateChanged(prior: File, current: File): Boolean =
    prior.eventDate != Date.getDefaultInstance() &&
      current.eventDate != Date.getDefaultInstance() &&
      prior.eventDate != current.eventDate

  private fun digestManifest(manifest: Map<String, ManifestEntry>): ByteString {
    val digest = MessageDigest.getInstance("SHA-256")
    for ((blobUri, entry) in manifest.toSortedMap()) {
      val uriBytes = blobUri.toByteArray(StandardCharsets.UTF_8)
      digest.update(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(uriBytes.size).array())
      digest.update(uriBytes)
      digest.update(ByteBuffer.allocate(Long.SIZE_BYTES).putLong(entry.file.blobGeneration).array())
      digest.update(
        ByteBuffer.allocate(Int.SIZE_BYTES * 3)
          .putInt(entry.file.eventDate.year)
          .putInt(entry.file.eventDate.month)
          .putInt(entry.file.eventDate.day)
          .array()
      )
    }
    return digest.digest().toByteString()
  }

  private fun ManifestEntry.toProto(
    ownerResourceId: (String) -> String
  ): InternalCandidate.ManifestEntry =
    InternalCandidateKt.manifestEntry {
      blobUri = file.blobUri
      blobGeneration = file.blobGeneration
      eventDate = file.eventDate
      outputSourceRawImpressionUploadResourceId = ownerResourceId(outputSourceRawImpressionUpload)
    }

  private fun Collection<Revision>.associateByUniqueName(): Map<String, Revision> = buildMap {
    for (revision in this@associateByUniqueName) {
      require(put(revision.rawImpressionUpload, revision) == null) {
        "Duplicate RawImpressionUpload ${revision.rawImpressionUpload}"
      }
    }
  }

  companion object {
    private val REVISION_COMPARATOR =
      compareBy<Revision> { it.doneBlobCreateTime ?: it.createTime }
        .thenBy { it.createTime }
        .thenBy { it.doneBlobGeneration }
        .thenBy { it.rawImpressionUpload }
  }
}
