# BENCH: adopt the pre-existing vid-models-storage bucket (orphaned from tf state by the stack
# wipe; the bucket survived deletion because it holds the compiled models). Import, do not recreate.
import {
  to = module.edp_aggregator.module.vid_models_bucket.google_storage_bucket.bucket
  id = "vid-models-storage"
}
