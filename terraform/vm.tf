
# Create a VM
resource "google_compute_instance" "vm_instance" {
  name         = "${local.prefix}-vm-bazel-machine"
  machine_type = "n1-standard-1"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-10"
    }
  }
  metadata_startup_script = data.template_file.user_data.rendered

  network_interface {
    network = "default"

    access_config {
      // Ephemeral IP address
    }
  }
      tags = ["http-server", "https-server"]
    }

    
  
  

