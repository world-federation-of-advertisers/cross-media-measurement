# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "null_resource" "set_initial_state" {
  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command = "echo \"0\" > current_state.txt"
  }
}

resource "null_resource" "build-images" {
  depends_on = [null_resource.set_initial_state]
  count = length(var.image_paths)

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command = "while [[ $(cat current_state.txt) != \"${count.index}\" ]]; do echo \"${count.index} is waiting...\";sleep 5;done"
  }

  provisioner "local-exec" {
    working_dir = "${var.path_to_cmm}"
    command = "tools/bazel-container run ${var.image_paths[count.index]} -c opt --define container_registry=gcr.io --define image_repo_prefix=${data.google_client_config.current.project}"
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command = "echo \"${count.index+1}\" > current_state.txt"
  }
}

resource "null_resource" "delete_initial_state" {
  depends_on = [null_resource.build-images]
  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command = "rm current_state.txt"
  }
}
