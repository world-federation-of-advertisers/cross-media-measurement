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



#resource "google_compute_network" "vpc_network" {
#  name = "terraform-network"
#}
/*

# Create a VPC network and subnetwork
resource "google_compute_network" "vpc_network" {
  name                    = "wfa-vpc-1"
  auto_create_subnetworks = false
}
/*
resource "google_compute_subnetwork" "vpc_subnet" {
  name          = "wfa-kingdom-private-subnet"
  network       = google_compute_network.vpc_network.self_link
  ip_cidr_range = "10.0.0.0/24"
  region        = "us-central1"
  
}

*/
