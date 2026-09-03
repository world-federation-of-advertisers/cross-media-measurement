# Reference VID impression upload guide

This guide describes how an Event Data Provider (EDP) submits reference VID impressions to the
VID-labeling pipeline and how to correct an upload that contains bad data. The market operator
provides the Cloud Storage bucket and raw-impression path assigned to the EDP.

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

**The EDP must not fix bad data by deleting or overwriting files and writing a new `done` generation
on its own.** Without eviction, the pipeline treats the new marker as an incremental upload and
does not remove all data and generated output from the earlier upload. This can leave stale
VID-labeled impressions and can corrupt the ordering required by memoized model lines.

Contact the market operator and wait for confirmation that the affected upload has been evicted
before modifying the directory. If the existing files are correct and the only problem is that
additional files were forgotten, use one of the backfill strategies above instead; eviction is not
required for a purely additive backfill.

When bad data is discovered:

1. Contact the market operator and provide:
   * the DataProvider resource name;
   * every affected date and `done` object URI.
2. Do not modify the raw-impression directory or its `done` object while the operator investigates
   or runs eviction.
3. Wait for the operator to confirm that eviction completed successfully.
4. Make each affected date directory contain the complete corrected dataset:
   * leave unchanged objects in place;
   * overwrite corrupted objects with corrected data;
   * add missing objects; and
   * remove objects that must no longer contribute impressions.
5. After the directory is final, overwrite its empty `done` object to create a new generation.
6. When correcting multiple dates, process them from oldest to newest and wait for each replacement
   to complete before writing the next `done` generation.

The replacement upload processes every object remaining in the directory, including unchanged
objects. Later dates evicted only because they depend on a corrected memoized rank-index state do
not need to be re-uploaded by the EDP; the market operator recovers those dates separately.

## Processing failures without bad data

Do not rewrite `done` merely because processing is delayed or failed. If the directory contents are
correct, report the affected date and upload to the market operator. The operator determines
whether the existing upload should be retried or evicted.

For the operator procedure, see
[Healing VID-labeling uploads](../operations/healing-vid-labeling-uploads.md).
