# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Create a VM
resource "google_compute_instance" "vm_instance" {
  name         = "${local.prefix}-vm-bazel-machine"
  machine_type = "n1-standard-4"
  zone         = "us-central1-a"
  tags = ["vm", "tf", "http-server", "https-server"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-10"
      size = "50"
    }
  }

  network_interface {
    network = "default"
    access_config {
    }
  }
  metadata_startup_script = "${file("packages.sh")}"
}

resource "null_resource" "deploy_files" {
  triggers = {
    dir_sha1 = sha1(filesha1("packages.sh"))
  }
}



