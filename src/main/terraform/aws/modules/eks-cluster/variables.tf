variable "aws_region" {
  description = "AWS region"
  type        = string
  nullable    = false
}

variable "cluster_name" {
  description = "name of the eks cluster"
  type        = string
  nullable    = false
}

variable "vpc_id" {
  description = "vpc id"
  type        = string
  nullable    = false
}

variable "subnet_ids" {
  description = "list of subnet ids"
  type        = list(string)
  nullable    = false
}

variable "control_plane_subnet_ids" {
  description = "list of subnet ids used by control plane"
  type        = list(string)
  nullable    = false
}
