# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

output "work_item_tee_mig_names" {
  description = "Names of Managed Instance Groups that consume Secure Computation WorkItems."
  value = concat(
    [module.result_fulfiller_tee_app.managed_instance_group_name],
    [for key in sort(keys(module.vid_labeling_tee_app)) :
      module.vid_labeling_tee_app[key].managed_instance_group_name
    ],
  )
}

output "vid_labeling_rollout_manifest" {
  description = "Terraform-owned function, worker and protocol/config identity for checked VID cutover."
  value = {
    schema_version   = 1
    project          = data.google_project.project.id
    region           = data.google_client_config.default.region
    spanner_instance = var.spanner_instance_name
    spanner_database = var.spanner_database_name
    contract_id      = local.vid_labeling_contract_id
    functions        = local.vid_labeling_rollout_functions
    queues = concat(
      [for key in sort(keys(var.vid_labeling_workers)) : "queues/${var.vid_labeling_workers[key].queue.topic_name}"],
      ["queues/data-availability-sync-queue"]
    )
    migs = [for key in sort(keys(var.vid_labeling_workers)) : var.vid_labeling_workers[key].worker.managed_instance_group_name]
  }
}
