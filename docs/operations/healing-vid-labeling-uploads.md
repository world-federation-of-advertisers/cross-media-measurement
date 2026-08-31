# Healing VID-labeling uploads

Use the `vid-labeling-heal evict-uploads` command when a completed raw-impression upload contains
bad data and must be replaced. The same command handles memoized and non-memoized model lines; the
operator supplies the bad upload resource names, not individual model lines.

## Before eviction

Pause DataWatcher delivery to both `VidLabelingDispatcher` and `DataAvailabilitySync`. Stop
`VidLabelingMonitor`, the ranker, and the VID-labeler workers. Drain or cancel affected queued and
running labeling work, and wait for in-flight data-availability sync invocations to finish. Leave
these components quiesced until the command finishes. The required `--pipeline-quiesced` flag is an
acknowledgement of this operational step; it does not stop the components itself.

Run the command with:

* `--bad-uploads`: comma-separated `RawImpressionUpload` resource names containing bad data.
* `--retention-days`: the bounded history in which the command may inspect and evict uploads.
* `--reason`: the diagnosis recorded on each failed upload/model-line row.
* `--labeled-impressions-blob-prefix`: the absolute URI prefix configured for VID-labeled output.
* `--gcs-project`: optional Google Cloud project for access to labeled-output buckets.
* the normal EDP Aggregator API and mutual-TLS flags.

Review the printed plan before entering `yes`. Non-memoized model lines include only the selected
uploads. Memoized model lines include the selected uploads and every later upload that depends on
their cumulative rank-index state.

## What eviction changes

For every planned upload/model-line pair, the command:

1. marks the `RawImpressionUploadModelLine` `FAILED`;
2. soft-deletes cumulative rank-index snapshots for memoized model lines;
3. soft-deletes the matching `ImpressionMetadata`;
4. permanently deletes the fetched generations of the generated VID-labeled blob and its
   `.metadata.binpb` sidecar.

Raw impression objects and `RawImpressionUploadFile` history are retained. Metadata is deleted
before GCS output, so the asynchronous data-availability cleanup is idempotent when it receives the
object-deletion event. Kingdom availability is not narrowed during the repair window because its
interval representation cannot express an interior missing day; Results Fulfiller ignores the
soft-deleted metadata.

## Re-upload corrected data

After eviction succeeds, the EDP corrects the retained raw-impression directory and writes a new
generation of its `done` object. This creates a replacement upload whose snapshot is compared with
the previous upload. Unchanged raw objects may be reused; changed and added objects are processed,
and removed objects stay excluded.

For a memoized cascade, re-trigger every evicted date in chronological order, beginning with the
earliest corrected date. Keep dispatch ordered so each cumulative rank-index snapshot is rebuilt
from its corrected predecessor. The normal labeling and data-availability flows regenerate output,
restore matching soft-deleted metadata, and publish availability to Kingdom. Do not run
`retry-failed` for rows evicted because their original jobs describe the invalid attempt.

Before replacement processing begins, the eviction operation is safe to repeat after a partial
failure. It skips rows already marked `FAILED`, already-deleted metadata, and output blobs that are
already absent. Do not repeat eviction after corrected output has been generated at the same
deterministic URI.
