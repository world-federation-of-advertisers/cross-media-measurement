
# Create a GKE cluster with one node
resource "google_container_cluster" "cluster" {
  name     = "cluster"
  location = "us-central1"

  # Add the VPC to the cluster
 # network = google_compute_network.vpc_network.name

  # Configure the cluster
  initial_node_count = 1
  node_config {
    machine_type = "n1-standard-1"
    disk_size_gb = 100
    disk_type    = "pd-standard"
  }

}


 