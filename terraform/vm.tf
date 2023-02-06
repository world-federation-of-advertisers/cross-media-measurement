/*

What is this VM meant for.

*/
resource "google_compute_instance" "vm_instance" {
  name = "${local.prefix}-vm-bazel-machine"
  machine_type = "e2-medium"
  zone = "us-central1-a"
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  metadata_startup_script = data.template_file.user_data.rendered

  network_interface {
    network = google_compute_network.vpc_network.name
    access_config {}
  }
}