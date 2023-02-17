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

#!/bin/bash
{
  wget https://github.com/bazelbuild/bazelisk/releases/download/v1.8.1/bazelisk-linux-amd64
  chmod +x bazelisk-linux-amd64
  sudo mv bazelisk-linux-amd64 /usr/local/bin/bazel

  # make sure you get the binary available in $PATH
  > which bazel
  bazel is /usr/local/bin/bazel

	sudo apt-get update
	sudo apt-get upgrade
	sudo apt-get install gnupg-curl


# install kubectl

  curl -LO https://dl.k8s.io/release/v1.26.0/bin/linux/amd64/kubectl

# Download the kubectl checksum file:
  curl -LO "https://dl.k8s.io/release/v1.26.0/bin/linux/amd64/kubectl.sha256"

# Validate the kubectl binary against the checksum file:
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check

# Install kubectl

  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install git

  sudo apt-get update
  sudo apt-get install git

# Install clang

  sudo apt install aptitude
  sudo aptitude install clang

# Install swig

  sudo apt-get install swig

# Install JDK

  sudo apt-get install openjdk-11-jdk
  sudo nano /etc/environment --->  JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
  source /etc/environment



 # echo "Installing docker compose ..."
 # wget -O /usr/local/bin/docker-compose "https://github.com/docker/compose/releases/download/v$DOCKER_COMPOSE_VERSION/docker-compose-linux-x86_64"
 # ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
 # chmod +x /usr/local/bin/docker-compose
} > /usr/tmp/startup.logs
