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
