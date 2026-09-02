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

## Backfill a newly onboarded advertiser

When a new advertiser needs historical data added to dates that were already processed, create a
dedicated subdirectory for that advertiser under each date. For example:

```text
gs://BUCKET/RAW_IMPRESSION_PREFIX/2026-08-15/advertiser-XYZ/
  campaign-1.parquet
  campaign-2.parquet
  done
```

The `done` object must be inside the advertiser subdirectory. Its directory is the upload boundary,
so the pipeline registers a new upload containing only the files for that advertiser. Do not write
the new `done` object in the date directory, because a marker there recursively includes files in
all of its subdirectories.

For each historical date:

1. Create a separate advertiser subdirectory under that date.
2. Write only the new advertiser's raw-impression files into that subdirectory.
3. Verify that the advertiser subdirectory contains the complete dataset intended for that date.
4. Write the empty `done` object into the advertiser subdirectory last.
5. Process dates from oldest to newest, waiting for each upload to complete before writing the next
   date's `done` object.

Do not modify the existing date upload or another advertiser's subdirectory. Multiple independent
uploads can contain impressions for the same date, and each upload can be corrected or evicted
independently.

For a legacy date whose original files and `done` marker are directly in the date directory, leave
the existing files unchanged and create the new advertiser subdirectory beneath it. Writing the
marker inside the advertiser subdirectory limits the new upload to that advertiser's files.

## Correct bad data

**Never overwrite or re-upload the `done` object for an already processed directory until the
market operator confirms that the bad upload has been evicted.** A new `done` generation starts a
replacement upload. Starting it before eviction can leave stale labeled output and can corrupt the
ordering required by memoized model lines.

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
