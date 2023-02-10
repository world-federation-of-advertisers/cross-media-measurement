/*

*/
locals {
  project = "wfa"
  tags = {
    Environment     = var.env
  }
  prefix      = "${var.env}-${local.project}"
  prefix_path = "${var.env}/${local.project}"
}

data "template_file" "user_data" {
  template = file("pre_install.sh.tpl")

  vars = {
    projectName = var.projectName
  }
}

variable projectName {
  default = "wfa"
  description = "The IAC project of WFA "
}

variable env {
  default = "dev"
  description = "Represents the environment used."
}

variable region {
  default = "us-central1"
  description = "Represents the environment used."
}

variable project {
  default = "my-project-test-373810"
  description = "The project ID"
}

variable gke_service_account_name {
  default = "kingdom"
  description = "kingdom SA"
}