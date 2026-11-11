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

output "vid_labeling_machine_api_service_account_subjects" {
  description = "Numeric Google ID-token subjects for the machine functions."
  value = {
    dispatcher         = [module.vid_labeling_dispatcher_cloud_function.cloud_function_service_account.unique_id]
    healing_controller = [module.vid_labeling_healing_controller_cloud_function.cloud_function_service_account.unique_id]
  }
}
