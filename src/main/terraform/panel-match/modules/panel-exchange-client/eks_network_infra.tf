# Copyright 2022 The Cross-Media Measurement Authors
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

resource "aws_vpc" "vpc" {
  cidr_block = var.vpc_cidr

  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "public_subnet" {
  count = var.availability_zones_count

  vpc_id            = aws_vpc.vpc.id
  cidr_block        = cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  map_public_ip_on_launch = true
}

resource "aws_subnet" "private_subnet" {
  count = var.availability_zones_count

  vpc_id            = aws_vpc.vpc.id
  cidr_block        = cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, count.index + var.availability_zones_count)
  availability_zone = data.aws_availability_zones.available.names[count.index]
}

resource "aws_internet_gateway" "gateway" {
  vpc_id     = aws_vpc.vpc.id
  depends_on = [aws_vpc.vpc]
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gateway.id
  }
}

resource "aws_route_table_association" "internet_access" {
  count = var.availability_zones_count

  subnet_id      = aws_subnet.public_subnet[count.index].id
  route_table_id = aws_route_table.main.id
}

resource "aws_eip" "main" {
  vpc = true
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.main.id
  subnet_id     = aws_subnet.public_subnet[0].id
}

resource "aws_route" "main" {
  route_table_id         = aws_vpc.vpc.default_route_table_id
  nat_gateway_id         = aws_nat_gateway.main.id
  destination_cidr_block = "0.0.0.0/0"
}

resource "aws_security_group" "public_sg" {
  name   = "${var.project}-Public-sg"
  vpc_id = aws_vpc.vpc.id
}

resource "aws_security_group_rule" "sg_ingress_public_443" {
  security_group_id = aws_security_group.public_sg.id
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "sg_ingress_public_80" {
  security_group_id = aws_security_group.public_sg.id
  type              = "ingress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "sg_egress_public" {
  security_group_id = aws_security_group.public_sg.id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group" "data_plane_sg" {
  name   = "${var.project}-Worker-sg"
  vpc_id = aws_vpc.vpc.id
}

resource "aws_security_group_rule" "nodes" {
  description       = "Allow nodes to communicate to each other"
  security_group_id = aws_security_group.data_plane_sg.id
  type              = "ingress"
  from_port         = 0
  to_port           = 65535
  protocol          = "-1"
  cidr_blocks = flatten([
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 0),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 1),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 2),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 3)
  ])
}

resource "aws_security_group_rule" "nodes_inbound" {
  description       = "Allow worker Kublets and pods to receive control plane communication"
  security_group_id = aws_security_group.data_plane_sg.id
  type              = "ingress"
  from_port         = 1025
  to_port           = 65535
  protocol          = "tcp"
  cidr_blocks = flatten([
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 2),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 3)
  ])
}

resource "aws_security_group_rule" "node_outbound" {
  security_group_id = aws_security_group.data_plane_sg.id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group" "control_plane_sg" {
  name   = "${var.project}-ControlPlane-sg"
  vpc_id = aws_vpc.vpc.id
}

resource "aws_security_group_rule" "control_plane_inbound" {
  security_group_id = aws_security_group.control_plane_sg.id
  type              = "ingress"
  from_port         = 0
  to_port           = 65535
  protocol          = "tcp"
  cidr_blocks = flatten([
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 0),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 1),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 2),
    cidrsubnet(var.vpc_cidr, var.subnet_cidr_bits, 3)
  ])
}

resource "aws_security_group_rule" "control_plane_outbound" {
  security_group_id = aws_security_group.control_plane_sg.id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
}
