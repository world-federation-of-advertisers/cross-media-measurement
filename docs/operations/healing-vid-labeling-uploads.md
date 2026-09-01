# Healing VID-labeling uploads

Use the `vid-labeling-heal evict-uploads` command when a completed raw-impression upload contains
bad data and must be replaced. The same command handles memoized and non-memoized model lines; the
operator supplies the bad upload resource names, not individual model lines.

## Before eviction

The command checks every upload/model-line under the DataProvider and refuses to run while any VID
labeling pipeline is queued or running. If this happens, retry later after processing for that
DataProvider has finished.

### Find uploads for an event date

`evict-uploads` accepts upload resource names rather than dates. To find every upload containing
raw-impression files for one or more bad dates, query the EDP Aggregator Spanner database:

```sql
SELECT DISTINCT
  CONCAT(
    'dataProviders/',
    U.DataProviderResourceId,
    '/rawImpressionUploads/',
    U.RawImpressionUploadResourceId
  ) AS RawImpressionUpload,
  F.EventDate,
  U.DoneBlobUri,
  U.State
FROM RawImpressionUploadFile AS F
JOIN RawImpressionUpload AS U
  USING (DataProviderResourceId, RawImpressionUploadId)
WHERE F.DataProviderResourceId = 'DATA_PROVIDER_RESOURCE_ID'
  AND F.EventDate IN (DATE '2026-08-15', DATE '2026-08-16')
ORDER BY F.EventDate, RawImpressionUpload;
```

Pass every applicable returned resource name to `--bad-uploads`. A date can belong to multiple
uploads, such as a main upload and one or more advertiser-backfill uploads, so do not select only
the first row for that date.

Run the command with:

* `--bad-uploads`: comma-separated `RawImpressionUpload` resource names containing bad data.
* `--retention-days`: the bounded history in which the command may inspect and evict uploads.
* `--reason`: the diagnosis recorded on each failed upload/model-line row.
* `--labeled-impressions-blob-prefix`: the absolute URI prefix configured for VID-labeled output.
* `--gcs-project`: optional Google Cloud project for access to labeled-output buckets.
* the normal EDP Aggregator API and mutual-TLS flags.

Review the printed plan before entering `yes`. Non-memoized model lines include only the selected
uploads. Memoized model lines include the selected uploads and every later upload that depends on
their cumulative rank-index state. The command refuses to evict a superseded upload revision while
a completed replacement owns the current deterministic output; select the replacement revision if
it is also invalid.

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
generation of its `done` object. This creates a replacement upload containing a complete snapshot
of every raw-impression object currently present in the directory. Unchanged objects do not need to
be uploaded again, but the pipeline processes them again together with changed and added objects.
Objects removed from the directory are excluded from the replacement upload.

For explicitly selected bad uploads, the EDP writes corrected data and a new `done` generation in
chronological order. That normal path recreates every applicable memoized and non-memoized model
line.

Later uploads pulled into the eviction only by a memoized cascade do not need their raw data
re-uploaded. After every earlier corrected upload has completed, run the ordered `recover-upload`
commands printed by `evict-uploads`. Superseded historical revisions are omitted from this list.
Each command takes one source upload plus the complete comma-separated set of memoized model lines
evicted from that revision, validates that the source is still latest and that every selected row is
`FAILED` with a deleted snapshot, then atomically writes a new generation of the existing empty
`done` object. The new object carries the selected model lines and source upload as paired recovery
metadata; DataWatcher forwards them in `X-Override-Model-Lines` and
`X-Recovery-Source-Upload`. Before
honoring the override, VidLabelingDispatcher independently verifies that the source is the latest
revision for that done path and that every selected row is `FAILED` with memoized snapshot history.
It then creates a replacement upload for only those model lines. For example:

```
vid-labeling-heal recover-upload \
  --raw-impression-upload=dataProviders/DP/rawImpressionUploads/D4_UPLOAD \
  --model-lines=modelProviders/MP/modelSuites/MS/modelLines/ML1 \
  --edpa-public-api-target=EDPA_TARGET \
  --tls-cert-file=TLS_CERT \
  --tls-key-file=TLS_KEY \
  --cert-collection-file=ROOT_CERTS \
  --gcs-project=GCS_PROJECT
```

Run recovery commands in their printed order and wait for each preceding replacement to complete,
so every cumulative rank-index snapshot is rebuilt from its corrected predecessor. The normal
labeling and data-availability flows regenerate output, restore matching soft-deleted metadata, and
publish availability to Kingdom. Do not run `retry-failed` for rows evicted because their original
jobs describe the invalid attempt. Recovery dispatch failures are returned by DataWatcher so the
Eventarc subscription retries them and ultimately preserves exhausted deliveries in its DLQ.

Before replacement processing begins, the eviction operation is safe to repeat after a partial
failure. It skips rows already marked `FAILED`, already-deleted metadata, and output blobs that are
already absent. Do not repeat eviction after corrected output has been generated at the same
deterministic URI.
