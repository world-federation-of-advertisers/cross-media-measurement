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

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier.Classification
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier.File
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier.Revision
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate

@RunWith(JUnit4::class)
class RawImpressionUploadManifestClassifierTest {
  private val classifier = RawImpressionUploadManifestClassifier()

  @Test
  fun `classify returns NEW without a predecessor`() {
    val result = classifier.classify("current", listOf(revision("current", 1, files = files("a"))))

    assertThat(result.classification).isEqualTo(Classification.NEW)
    assertThat(result.priorManifest).isEmpty()
  }

  @Test
  fun `classify returns NO_OP for an unchanged manifest`() {
    val result = classify(files("a", "b"), files("a", "b"))

    assertThat(result.classification).isEqualTo(Classification.NO_OP)
    assertThat(result.differences).isEmpty()
  }

  @Test
  fun `classify returns APPEND for only new URIs`() {
    val result = classify(files("a"), files("a", "b"))

    assertThat(result.classification).isEqualTo(Classification.APPEND)
  }

  @Test
  fun `classify returns EDITED for only changed generations`() {
    val result = classify(files("a"), listOf(file("a", generation = 2L)))

    assertThat(result.classification).isEqualTo(Classification.EDITED)
    assertThat(result.differences.single().prior?.ownerRawImpressionUpload).isEqualTo("previous")
  }

  @Test
  fun `classify returns REMOVED for only missing URIs`() {
    val result = classify(files("a", "b"), files("a"))

    assertThat(result.classification).isEqualTo(Classification.REMOVED)
  }

  @Test
  fun `classify returns MIXED when change categories are combined`() {
    val cases =
      listOf(
        files("a") to listOf(file("a", 2L), file("b")),
        files("a") to files("b"),
        files("a", "b") to listOf(file("a", 2L)),
        files("a", "removed") to listOf(file("a", 2L), file("c")),
      )

    for ((prior, current) in cases) {
      val result = classify(prior, current)
      assertThat(result.classification).isEqualTo(Classification.MIXED)
    }
  }

  @Test
  fun `reconstructPriorManifest maps each URI to its latest owner`() {
    val revisions =
      listOf(
        revision("base", 1, files = files("a", "b")),
        revision("edit", 2, replaces = "base", files = listOf(file("a", 2L))),
        revision("append", 3, replaces = "edit", files = files("c")),
        revision("current", 4, replaces = "append", files = files("a", "b", "c", "d")),
      )

    val prior = classifier.reconstructPriorManifest(revisions.last(), revisions.shuffled())

    assertThat(prior.getValue("gs://raw/a").ownerRawImpressionUpload).isEqualTo("edit")
    assertThat(prior.getValue("gs://raw/b").ownerRawImpressionUpload).isEqualTo("base")
    assertThat(prior.getValue("gs://raw/c").ownerRawImpressionUpload).isEqualTo("append")
  }

  @Test
  fun `reconstructEffectiveManifest includes prior additive revisions`() {
    val revisions =
      listOf(
        revision("base", 1, files = files("a", "b")),
        revision("append", 2, replaces = "base", files = files("c")),
      )

    val manifest = classifier.reconstructEffectiveManifest(revisions.last(), revisions)

    assertThat(manifest.keys).containsExactly("gs://raw/a", "gs://raw/b", "gs://raw/c")
  }

  @Test
  fun `reconstructEffectiveManifest ignores mutable failure state`() {
    val revisions =
      listOf(
        revision("base", 1, failed = true, files = files("a", "b")),
        revision("append", 2, replaces = "base", failed = true, files = files("c")),
        revision("latest", 3, replaces = "append", failed = true, files = files("d")),
      )

    val manifest = classifier.reconstructEffectiveManifest(revisions.last(), revisions)

    assertThat(manifest.keys)
      .containsExactly("gs://raw/a", "gs://raw/b", "gs://raw/c", "gs://raw/d")
  }

  @Test
  fun `reconstructPriorManifest stops at post-eviction full snapshot`() {
    val revisions =
      listOf(
        revision("old", 1, files = files("a", "removed")),
        revision(
          "replacement",
          2,
          replaces = "old",
          uploadHealingOperation = "operation",
          files = files("a"),
        ),
        revision("append", 3, replaces = "replacement", files = files("b")),
        revision("current", 4, replaces = "append", files = files("a", "b")),
      )

    val result = classifier.classify("current", revisions)

    assertThat(result.classification).isEqualTo(Classification.NO_OP)
    assertThat(result.priorManifest).doesNotContainKey("gs://raw/removed")
  }

  @Test
  fun `superseding correction compares with the last healthy manifest`() {
    val revisions =
      listOf(
        revision("healthy", 1, files = files("a", "removed")),
        revision("quarantined", 2, replaces = "healthy", quarantined = true, files = files("a")),
        revision("current", 3, replaces = "quarantined", files = files("a", "added")),
      )

    val result = classifier.classify("current", revisions)

    assertThat(result.classification).isEqualTo(Classification.MIXED)
    assertThat(result.priorManifest.keys).containsExactly("gs://raw/a", "gs://raw/removed")
    assertThat(result.priorManifest.getValue("gs://raw/removed").ownerRawImpressionUpload)
      .isEqualTo("healthy")
  }

  @Test
  fun `quarantined revision is a complete effective snapshot`() {
    val revisions =
      listOf(
        revision("healthy", 1, files = files("a", "removed")),
        revision("quarantined", 2, replaces = "healthy", quarantined = true, files = files("a")),
      )

    val manifest = classifier.reconstructEffectiveManifest(revisions.last(), revisions)

    assertThat(manifest.keys).containsExactly("gs://raw/a")
  }

  @Test
  fun `reconstructPriorManifest treats a revision after failure as a full snapshot`() {
    val revisions =
      listOf(
        revision("failed", 1, failed = true, files = files("removed")),
        revision("replacement", 2, replaces = "failed", files = files("a")),
        revision("current", 3, replaces = "replacement", files = files("a")),
      )

    val result = classifier.classify("current", revisions)

    assertThat(result.classification).isEqualTo(Classification.NO_OP)
    assertThat(result.priorManifest).doesNotContainKey("gs://raw/removed")
  }

  @Test
  fun `classify orders legacy revisions by create time`() {
    val revisions =
      listOf(
        revision("current", 3, doneBlobCreateTime = null, files = files("a", "b", "c")),
        revision("base", 1, doneBlobCreateTime = null, files = files("a")),
        revision("append", 2, doneBlobCreateTime = null, files = files("b")),
      )

    val result = classifier.classify("current", revisions)

    assertThat(result.classification).isEqualTo(Classification.APPEND)
    assertThat(result.priorManifest.keys).containsExactly("gs://raw/a", "gs://raw/b")
  }

  @Test
  fun `classify treats a legacy unknown generation as edited`() {
    val result = classify(listOf(file("a", generation = 0L)), files("a"))

    assertThat(result.classification).isEqualTo(Classification.EDITED)
  }

  @Test
  fun `manifest digests are stable across input ordering`() {
    val ordered =
      listOf(
        revision("previous", 1, files = files("a", "b")),
        revision("current", 2, replaces = "previous", files = files("a", "b", "c")),
      )
    val reversedFiles =
      ordered.map { revision -> revision.copy(files = revision.files.reversed()) }.reversed()

    val first = classifier.classify("current", ordered)
    val second = classifier.classify("current", reversedFiles)

    assertThat(second.priorManifestDigest).isEqualTo(first.priorManifestDigest)
    assertThat(second.currentManifestDigest).isEqualTo(first.currentManifestDigest)
    assertThat(first.priorManifestDigest).hasSize(32)
    assertThat(first.currentManifestDigest).hasSize(32)
    assertThat(first.currentManifestDigest).isNotEqualTo(first.priorManifestDigest)
  }

  @Test
  fun `snapshot captures complete manifests differences and normalized owner IDs`() {
    val previous = "dataProviders/dp/rawImpressionUploads/previous"
    val current = "dataProviders/dp/rawImpressionUploads/current"
    val result =
      classifier.classify(
        current,
        listOf(
          revision(previous, 1, files = files("a", "b")),
          revision(current, 2, replaces = previous, files = listOf(file("a", 2L), file("c"))),
        ),
      )

    val snapshot = classifier.snapshot(result) { it.substringAfterLast('/') }

    assertThat(snapshot.priorManifestList.map { it.blobUri })
      .containsExactly("gs://raw/a", "gs://raw/b")
      .inOrder()
    assertThat(snapshot.currentManifestList.map { it.blobUri })
      .containsExactly("gs://raw/a", "gs://raw/c")
      .inOrder()
    assertThat(snapshot.differencesList.map { it.type })
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_EDITED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_REMOVED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_ADDED,
      )
      .inOrder()
    assertThat(snapshot.differencesList[0].prior.ownerRawImpressionUploadResourceId)
      .isEqualTo("previous")
    assertThat(snapshot.differencesList[0].current.ownerRawImpressionUploadResourceId)
      .isEqualTo("current")
  }

  @Test
  fun `classify rejects duplicate blob URI in a revision`() {
    val duplicate = revision("current", 1, files = listOf(file("a"), file("a", 2L)))

    assertFailsWith<IllegalArgumentException> { classifier.classify("current", listOf(duplicate)) }
  }

  private fun classify(
    priorFiles: List<File>,
    currentFiles: List<File>,
  ): RawImpressionUploadManifestClassifier.Result =
    classifier.classify(
      "current",
      listOf(
        revision("previous", 1, files = priorFiles),
        revision("current", 2, replaces = "previous", files = currentFiles),
      ),
    )

  private fun revision(
    name: String,
    order: Long,
    replaces: String = "",
    uploadHealingOperation: String = "",
    failed: Boolean = false,
    quarantined: Boolean = false,
    doneBlobCreateTime: Instant? = Instant.ofEpochSecond(order),
    files: List<File>,
  ) =
    Revision(
      rawImpressionUpload = name,
      doneBlobUri = DONE_BLOB_URI,
      doneBlobGeneration = order,
      doneBlobCreateTime = doneBlobCreateTime,
      createTime = Instant.ofEpochSecond(order),
      replacesRawImpressionUpload = replaces,
      uploadHealingOperation = uploadHealingOperation,
      failed = failed,
      quarantined = quarantined,
      files = files,
    )

  private fun files(vararg uris: String): List<File> = uris.map(::file)

  private fun file(uri: String, generation: Long = 1L) = File("gs://raw/$uri", generation)

  companion object {
    private const val DONE_BLOB_URI = "gs://raw/done"
  }
}
