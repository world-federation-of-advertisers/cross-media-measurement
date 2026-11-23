# Configuration selection is checked with mock providers and plan-only runs.
override_resource {
  override_during = plan
  target          = google_storage_bucket_object.upload_data_watcher_config
  values          = { generation = 111 }
}
override_resource {
  override_during = plan
  target          = google_storage_bucket_object.upload_data_watcher_dispatch_config
  values          = { generation = 222 }
}
mock_provider "google" {
  mock_data "google_client_config" {
    defaults = { project = "dispatch-test", region = "us-central1", zone = "us-central1-a" }
  }
  mock_data "google_project" {
    defaults = { project_id = "dispatch-test", id = "projects/dispatch-test", number = "123456789" }
  }
}
variables {
  pubsub_iam_service_account_member                    = "serviceAccount:publisher@dispatch-test.iam.gserviceaccount.com"
  vid_labeling_trace_operator_member                   = "serviceAccount:operator@dispatch-test.iam.gserviceaccount.com"
  requisition_fulfiller_config                         = { "queue" : { "subscription_name" : "dispatch-test", "topic_name" : "dispatch-test", "ack_deadline_seconds" : 600, "max_delivery_attempts" : 1, "minimum_backoff" : "10s", "maximum_backoff" : "600s" }, "worker" : { "instance_template_name" : "dispatch-test", "base_instance_name" : "dispatch-test", "managed_instance_group_name" : "dispatch-test", "mig_service_account_name" : "dispatch-test", "single_instance_assignment" : 1, "min_replicas" : 0, "max_replicas" : 1, "machine_type" : "dispatch-test", "java_tool_options" : null, "max_attempt_runtime_seconds" : null, "stack_dump_interval_seconds" : null, "docker_image" : "dispatch-test", "mig_distribution_policy_zones" : ["us-central1-a"], "app_flags" : [] } }
  edpa_tee_app_tls_key                                 = { "secret_id" : "edpa-tee-app-tls-key", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  edpa_tee_app_tls_pem                                 = { "secret_id" : "edpa-tee-app-tls-pem", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  data_watcher_tls_key                                 = { "secret_id" : "data-watcher-tls-key", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  data_watcher_tls_pem                                 = { "secret_id" : "data-watcher-tls-pem", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  data_availability_tls_key                            = { "secret_id" : "data-availability-tls-key", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  data_availability_tls_pem                            = { "secret_id" : "data-availability-tls-pem", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  secure_computation_root_ca                           = { "secret_id" : "secure-computation-root-ca", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  metadata_storage_root_ca                             = { "secret_id" : "metadata-storage-root-ca", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  trusted_root_ca_collection                           = { "secret_id" : "trusted-root-ca-collection", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  edps_certs                                           = {}
  data_availability_sync_work_item_queue               = { "topic_name" : "data-availability-sync-work-item-queue", "subscription_name" : "data-availability-sync-work-item-queue-subscription", "ack_deadline_seconds" : 600, "max_delivery_attempts" : 100, "minimum_backoff" : "10s", "maximum_backoff" : "600s" }
  edp_aggregator_bucket_name                           = "dispatch-test"
  config_files_bucket_name                             = "dispatch-test"
  vid_models_bucket_name                               = "dispatch-test"
  data_watcher_config                                  = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "data-watcher-config.textproto" }
  data_watcher_delete_config                           = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "data-watcher-delete-config.textproto" }
  requisition_fetcher_config                           = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "requisition-fetcher-config.textproto" }
  edps_config                                          = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "edps-config.textproto" }
  event_group_sync_config                              = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "event-group-sync-config.textproto" }
  data_availability_sync_config                        = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "data-availability-sync-config.textproto" }
  results_fulfiller_event_descriptor                   = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "results-fulfiller-event-descriptor.textproto" }
  results_fulfiller_population_spec                    = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "results-fulfiller-population-spec.textproto" }
  vid_labeler_population_spec                          = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "vid-labeler-population-spec.textproto" }
  edp_aggregator_buckets_location                      = "US"
  data_watcher_service_account_name                    = "dispatch-test"
  data_watcher_trigger_service_account_name            = "dispatch-test"
  data_watcher_delete_service_account_name             = "dispatch-test"
  data_watcher_delete_trigger_service_account_name     = "dispatch-test"
  terraform_service_account                            = "terraform@dispatch-test.iam.gserviceaccount.com"
  requisition_fetcher_service_account_name             = "dispatch-test"
  event_group_sync_service_account_name                = "dispatch-test"
  data_availability_sync_service_account_name          = "dispatch-test"
  data_availability_cleanup_service_account_name       = "dispatch-test"
  event_group_sync_function_name                       = "dispatch-test"
  data_availability_sync_function_name                 = "dispatch-test"
  data_availability_cleanup_function_name              = "dispatch-test"
  cloud_function_configs                               = { "data_availability_cleanup" : { "function_name" : "data-availability-cleanup", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "data_availability_monitor" : { "function_name" : "data-availability-monitor", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "data_availability_sync" : { "function_name" : "data-availability-sync", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "data_watcher" : { "function_name" : "data-watcher", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "data_watcher_delete" : { "function_name" : "data-watcher-delete", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "event_group_sync" : { "function_name" : "event-group-sync", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "requisition_fetcher" : { "function_name" : "requisition-fetcher", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "vid_labeling_dispatcher" : { "function_name" : "vid-labeling-dispatcher", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "vid_labeling_healing_controller" : { "function_name" : "vid-labeling-healing-controller", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" }, "vid_labeling_monitor" : { "function_name" : "vid-labeling-monitor", "entry_point" : "org.example.Function", "extra_env_vars" : "CONFIG_BLOB_KEY=data-watcher-config.textproto,DATA_WATCHER_CONFIG_BLOB_KEY=data-watcher-config.textproto", "secret_mappings" : "", "uber_jar_path" : "../../cmms/envs/dev/data-watcher-config.textproto" } }
  requisition_fetcher_scheduler_config                 = { "schedule" : "*/5 * * * *", "time_zone" : "UTC", "name" : "dispatch-test", "function_url" : "https://example.test/function", "scheduler_sa_display_name" : "dispatch-test", "scheduler_sa_description" : "dispatch-test", "scheduler_job_description" : "dispatch-test", "paused" : null }
  edp_aggregator_service_account_name                  = "dispatch-test"
  spanner_instance                                     = { "name" : "dispatch-test" }
  requisition_fetcher_tls_key                          = { "secret_id" : "requisition-fetcher-tls-key", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  requisition_fetcher_tls_pem                          = { "secret_id" : "requisition-fetcher-tls-pem", "secret_local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "is_binary_format" : false }
  data_availability_cleanup_work_item_queue            = { "topic_name" : "data-availability-cleanup-work-item-queue", "subscription_name" : "data-availability-cleanup-work-item-queue-subscription", "ack_deadline_seconds" : 600, "max_delivery_attempts" : 100, "minimum_backoff" : "10s", "maximum_backoff" : "600s" }
  data_availability_monitor_service_account_name       = "dispatch-test"
  data_availability_monitor_config                     = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "data-availability-monitor-config.textproto" }
  data_availability_monitor_scheduler_config           = { "schedule" : "*/5 * * * *", "time_zone" : "UTC", "name" : "dispatch-test", "function_url" : "https://example.test/function", "scheduler_sa_display_name" : "dispatch-test", "scheduler_sa_description" : "dispatch-test", "scheduler_job_description" : "dispatch-test", "scheduler_job_name" : null }
  vid_labeling_dispatcher_service_account_name         = "dispatch-test"
  vid_labeling_monitor_service_account_name            = "dispatch-test"
  vid_labeling_healing_controller_service_account_name = "dispatch-test"
  vid_labeling_healing_labeled_output_object_prefixes  = ["projects/_/buckets/dispatch-test/objects/test/"]
  vid_labeling_dispatcher_config                       = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "vid-labeling-dispatcher-config.textproto" }
  vid_labeling_monitor_config                          = { "local_path" : "../../cmms/envs/dev/data-watcher-config.textproto", "destination" : "vid-labeling-monitor-config.textproto" }
  vid_labeling_monitor_scheduler_config                = { "schedule" : "*/5 * * * *", "time_zone" : "UTC", "name" : "dispatch-test", "function_url" : "https://example.test/function", "scheduler_sa_display_name" : "dispatch-test", "scheduler_sa_description" : "dispatch-test", "scheduler_job_description" : "dispatch-test", "scheduler_job_name" : null }
  vid_labeling_dispatch_scheduler_config               = { "schedule" : "*/5 * * * *", "time_zone" : "UTC", "name" : "dispatch-test", "function_url" : "https://example.test/function", "scheduler_sa_display_name" : "dispatch-test", "scheduler_sa_description" : "dispatch-test", "scheduler_job_description" : "dispatch-test", "scheduler_job_name" : null }
  vid_labeling_healing_controller_scheduler_config     = { "paused" : null, "schedule" : "*/5 * * * *", "time_zone" : "UTC", "name" : "dispatch-test", "function_url" : "https://example.test/function", "scheduler_sa_display_name" : "dispatch-test", "scheduler_sa_description" : "dispatch-test", "scheduler_job_description" : "dispatch-test", "scheduler_job_name" : null, "attempt_deadline" : null }
  event_group_sync_work_item_queue                     = { "topic_name" : "event-group-sync-work-item-queue", "subscription_name" : "event-group-sync-work-item-queue-subscription", "ack_deadline_seconds" : 600, "max_delivery_attempts" : 100, "minimum_backoff" : "10s", "maximum_backoff" : "600s" }
  tee_consumers_enabled                                = false
  vid_labeling_dispatch_work_item_queue                = { "topic_name" : "vid-labeling-dispatch-work-item-queue", "subscription_name" : "vid-labeling-dispatch-work-item-queue-subscription", "ack_deadline_seconds" : 600, "max_delivery_attempts" : 100, "minimum_backoff" : "10s", "maximum_backoff" : "600s" }
}

run "prepare" {
  command = plan
  variables { activate_vid_labeling_dispatch_config = false }
  assert {
    condition     = google_storage_bucket_object.upload_data_watcher_config.name == "data-watcher-config.textproto" && google_storage_bucket_object.upload_data_watcher_dispatch_config.name == "data-watcher-config.dispatch-v2.textproto"
    error_message = "Both legacy and v2 objects must remain provisioned."
  }
  assert {
    condition     = local.active_data_watcher_config_generation == 111
    error_message = "Preparation must bind both cached consumers to the legacy config generation."
  }
  assert {
    condition     = local.active_data_watcher_config_key == "data-watcher-config.textproto"
    error_message = "Preparation and activation must select distinct configuration keys."
  }
}

run "activate" {
  command = plan
  variables { activate_vid_labeling_dispatch_config = true }
  assert {
    condition     = google_storage_bucket_object.upload_data_watcher_config.name == "data-watcher-config.textproto" && google_storage_bucket_object.upload_data_watcher_dispatch_config.name == "data-watcher-config.dispatch-v2.textproto"
    error_message = "Both legacy and v2 objects must remain provisioned."
  }
  assert {
    condition     = local.active_data_watcher_config_generation == 222
    error_message = "Activation must roll both cached consumers with the v2 config generation."
  }
  assert {
    condition     = local.active_data_watcher_config_key == "data-watcher-config.dispatch-v2.textproto"
    error_message = "Preparation and activation must select distinct configuration keys."
  }
}
