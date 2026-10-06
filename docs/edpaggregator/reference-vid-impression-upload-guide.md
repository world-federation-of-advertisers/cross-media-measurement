# Reference VID impression upload guide

This guide describes how an Event Data Provider (EDP) submits reference VID impressions to the
VID-labeling pipeline and how to correct or permanently remove an upload that contains bad data.
The market operator provides the Cloud Storage bucket and raw-impression path assigned to the EDP.

## Submit a new date

For each date:

1. Write every raw-impression file into the assigned date directory.
2. Verify that the directory contains the complete dataset intended for that date.
3. Write an empty object named `done` into the directory last.
4. Do not add, replace, or remove objects after writing `done`.

The `done` object tells the VID-labeling pipeline that the directory is complete. The pipeline
records a snapshot of every raw-impression object currently present and processes that complete
snapshot for the applicable model lines.

## Backfill additional data

Backfilling is appropriate when the data already processed is correct, but additional raw
impressions must be added. Examples include a newly onboarded advertiser or files that the EDP
forgot to include in the original upload.

The system supports two backfill strategies. **Using a dedicated subdirectory is strongly
recommended** because it creates a clear upload boundary and allows the backfill to be corrected or
evicted independently from the original upload.

### Recommended: use a dedicated subdirectory

Create a dedicated subdirectory for the backfill under each affected date. For example:

```text
gs://BUCKET/RAW_IMPRESSION_PREFIX/2026-08-15/advertiser-XYZ/
  campaign-1.parquet
  campaign-2.parquet
  done
```

The `done` object must be inside the backfill subdirectory. Its directory is the upload boundary, so
the pipeline registers a new upload containing only the files in that subdirectory. Do not write
this marker in the date directory, because a marker there recursively includes files in all of its
subdirectories.

For each historical date:

1. Create a separate backfill subdirectory under that date.
2. Write only the additional raw-impression files into that subdirectory.
3. Verify that the subdirectory contains the complete backfill dataset intended for that date.
4. Write the empty `done` object into the backfill subdirectory last.
5. Process dates from oldest to newest, waiting for each upload to complete before writing the next
   date's `done` object.

Multiple independent uploads can contain impressions for the same date, and each upload can be
corrected or evicted independently.

For a legacy date whose original files and `done` marker are directly in the date directory, leave
the existing files unchanged and create the new backfill subdirectory beneath it. Writing the
marker inside the subdirectory limits the new upload to the backfill files.

### Alternative: add files to the existing directory

The EDP may instead add the missing raw-impression files to the existing directory and write a new
generation of its `done` object. The pipeline compares object URIs and generations with previously
registered files and creates a new upload containing the newly added object versions.

Use this strategy only for additive backfills: leave every previously processed file unchanged and
do not remove any file. Removing or correcting previously processed data requires operator-managed
eviction as described below.

## Correct bad data

Bad data includes situations such as:

* a file that should not have been uploaded;
* a corrupted or incomplete file;
* a file containing incorrect impressions, campaign data, or dates; or
* a previously processed file that must be replaced or removed.

The EDP corrects bad data by first making the affected directory contain its intended final state,
then writing a new `done` generation. The pipeline compares the complete directory with its
effective registered history. Pure additions continue normally; edits, removals, and mixed changes
are quarantined as correction candidates and create no labeling work before approval.

For a corrected replacement:

1. Make the directory contain the complete corrected dataset:
   * leave unchanged objects in place;
   * overwrite corrupted objects with corrected data;
   * add missing objects; and
   * remove objects that must no longer contribute impressions.
2. Overwrite the empty `done` object after the directory is final.
3. Do not modify that directory again while its correction is pending or healing.

To remove an upload permanently, remove every raw object from its directory and write a new `done`
generation. An empty initial upload is invalid, but an empty revision of a previously registered
directory is a removal candidate.

The first pending correction fences the DataProvider. Later uploads may still be registered, but
they remain deferred and no ordinary labeling work starts until correction completes. Additional
non-additive revisions become candidates in the same draft plan; a newer generation for the same
directory supersedes its older pending candidate.

The operator inspects the complete computed plan and makes one decision per candidate:

* **CORRECT:** evict derived state and replay the approved exact `done` generation.
* **NO_REPLACEMENT:** evict the upload without replaying it.

After approval, eviction, replay, dependent recovery, and fence release are automatic. The EDP does
not wait for an operator-managed eviction before publishing the corrected manifest, and the
operator does not ask the EDP to rewrite `done` during healing.

The replacement upload processes every object remaining in the directory, including unchanged
objects. Later dates evicted only because they depend on corrected memoized state are recovered
automatically from their persisted upload history.

## Processing failures without bad data

Do not rewrite `done` merely because processing is delayed or failed. If the directory contents are
correct, report the affected date and upload to the market operator. The operator determines
whether the existing upload should be retried or evicted.

For the operator procedure, see
[Healing VID-labeling uploads](../operations/healing-vid-labeling-uploads.md).
