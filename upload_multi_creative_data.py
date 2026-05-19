#!/usr/bin/env python3
"""
Fix blob_uri in multi-entity-key metadata.binpb files and upload to GCS.
"""
import subprocess
import os

BUCKET = "secure-computation-storage-dev-bucket"
LOCAL_BASE = "/tmp/multi-entity-local/bucket/edp7/ds"
GCS_PREFIX = f"gs://{BUCKET}/edp/edp7"
DATES = [f"2021-03-{d}" for d in range(15, 22)]
SUBPATH = "model-line/someLine/event-group-reference-id/edpa-eg-multi-creative-1"

for date in DATES:
    local_dir = f"{LOCAL_BASE}/{date}/{SUBPATH}"
    meta_path = f"{local_dir}/metadata.binpb"
    imp_path = f"{local_dir}/impressions"

    if not os.path.exists(meta_path):
        print(f"SKIP: {meta_path} not found")
        continue

    with open(meta_path, "rb") as f:
        data = f.read()

    old_uri = f"file:///edp7/ds/{date}/{SUBPATH}/impressions"
    new_uri = f"gs://{BUCKET}/edp/edp7/{date}/impressions-multi-creative"

    old_bytes = old_uri.encode("utf-8")
    new_bytes = new_uri.encode("utf-8")

    if old_bytes not in data:
        print(f"ERROR: old URI not found in {meta_path}")
        print(f"  looking for: {old_uri}")
        continue

    old_len = len(old_bytes)
    new_len = len(new_bytes)

    pos = data.index(old_bytes)
    # Length varint is right before the string
    if old_len < 128:
        assert data[pos - 1] == old_len, f"Expected length {old_len}, got {data[pos-1]}"
        assert data[pos - 2] == 0x0a, f"Expected tag 0x0a, got {hex(data[pos-2])}"
        if new_len < 128:
            new_data = data[:pos - 1] + bytes([new_len]) + new_bytes + data[pos + old_len:]
        else:
            new_data = data[:pos - 1] + bytes([new_len & 0x7f | 0x80, new_len >> 7]) + new_bytes + data[pos + old_len:]
    else:
        print(f"ERROR: old URI length {old_len} >= 128, need multi-byte varint handling")
        continue

    fixed_meta = f"{local_dir}/metadata-multi-creative.binpb"
    with open(fixed_meta, "wb") as f:
        f.write(new_data)

    # Upload impressions
    gcs_imp = f"{GCS_PREFIX}/{date}/impressions-multi-creative"
    subprocess.run(["gcloud", "storage", "cp", imp_path, gcs_imp], check=True)

    # Upload fixed metadata
    gcs_meta = f"{GCS_PREFIX}/{date}/metadata-multi-creative.binpb"
    subprocess.run(["gcloud", "storage", "cp", fixed_meta, gcs_meta], check=True)

    print(f"OK: {date}")

print("Done!")
