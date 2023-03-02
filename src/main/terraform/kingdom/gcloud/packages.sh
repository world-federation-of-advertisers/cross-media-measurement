#!/bin/bash
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

install_git(){
  # Install Git and test git Installation
  sudo apt-get update
  sudo apt-get install -y git

  echo "Check GIT Installation"
  git --version
}

install_python3(){
  # Install Python
  apt-get install -y python3

  echo "Check Python Installation"
  python3 --version
}

install_clang(){
  # Install clang
  sudo apt-get install -y aptitude
  sudo aptitude install -y clang

  echo "Check clang Installation"
  clang --version
}

install_swig(){
  # Install swig
  sudo apt-get install -y swig

  echo "Check swig Installation"
  swig -version
}

install_jdk(){
  # Install JDK
  sudo apt-get install -y openjdk-11-jdk
  echo "JAVA_HOME=\"/usr/lib/jvm/java-11-openjdk-amd64\"" >> /etc/environment
  source /etc/environment

  echo "Check JDK Installation"
  java --version
}

install_kubectl(){

  # install kubectl
  curl -LO "https://dl.k8s.io/release/v1.26.0/bin/linux/amd64/kubectl"

  # Download the kubectl checksum file:
  curl -LO "https://dl.k8s.io/release/v1.26.0/bin/linux/amd64/kubectl.sha256"

  # Validate the kubectl binary against the checksum file:
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check

  # Install kubectl
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

  echo "Check Kubectl Installation"
  kubectl
}

install_bazel(){
  # Install wget
  sudo apt-get install -y wget

  sudo wget https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-linux-amd64
  sudo chmod +x bazelisk-linux-amd64
  sudo mv bazelisk-linux-amd64 /usr/local/bin/bazelisk
}

clone_repo(){
  cd /tmp
  echo "Cloning the Repository"
  git clone -b main https://github.com/world-federation-of-advertisers/cross-media-measurement.git
  git clone -b main https://github.com/world-federation-of-advertisers/terraform-gcp-wfa-kingdom.git
}

bazel_build(){
  echo "Executing command to Build artifact"
  sudo cp -rf /tmp/terraform-gcp-wfa-kingdom/terraform/build_image.sh /tmp/cross-media-measurement/build_image.sh
  sudo cp -rf /tmp/terraform-gcp-wfa-kingdom/terraform/push_image.sh /tmp/cross-media-measurement/push_image.sh
  cd /tmp/cross-media-measurement
  sudo chmod +x /tmp/cross-media-measurement/build_image.sh
  sudo chmod +x /tmp/cross-media-measurement/push_image.sh
  echo "* * * * * cd /tmp/cross-media-measurement && sudo ./build_image.sh" | sudo crontab -
  echo "Waiting on crontab to be picked..."
  sleep 120
  logfile=$(sudo find /root/.cache/bazel/_bazel_root/ -name command.log)
  echo "The Log file for the Bazel command is $logfile"
  if [ "$logfile" == "" ]; then
    echo "Image build failed for some reason."
    return 127
  fi
  echo "Detailed Logs can be found in $logfile"
  until completed
  do
    sudo grep "Build completed successfully," $logfile
    if [ $? -ne 0 ]; then
      date
      echo "$(date) - $(tail -1 $logfile)"
      echo "Sleeping for 1 minute... check again in 1 minutes."
      sleep 60
      continue
    else
      echo "Completed Image Build Successfully"
      completed=0
      break
    fi
  done
}

bazel_push(){
  echo "Executing command to push to GCR"
  sleep 30
  echo "* * * * * cd /tmp/cross-media-measurement && sudo ./push_image.sh" | sudo crontab -
}


{
  set -x && chmod +x

  sudo mkdir -p /tmp/logs

  printf "\n Starting the initialization script. \n"

  # Git Installation
  {
    printf "\n Installing Git..."
    install_git > /tmp/git_installation.log
    if [ $? -eq 0 ]; then
      echo "Git installed successfully."
    else
      echo "Git installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/git_installation.log \n"
  }

  # Python Installation
  {
    printf "\n Installing Python..."
    install_python3 > /tmp/python_installation.log
    if [ $? -eq 0 ]; then
      echo "Python installed successfully."
    else
      echo "Python installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/python_installation.log \n"
  }

  # Clang Installation
  {
    printf "\n Installing clang..."
    install_clang > /tmp/clang_installation.log
    if [ $? -eq 0 ]; then
      echo "clang installed successfully."
    else
      echo "clang installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/clang_installation.log \n"
  }

  # Swig Installation
  {
    printf "\n Installing swig..."
    install_swig > /tmp/swig_installation.log
    if [ $? -eq 0 ]; then
      echo "swig installed successfully."
    else
      echo "swig installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/swig_installation.log \n"
  }

  # JDK Installation
  {
    printf "\n Installing JDK..."
    install_jdk > /tmp/jdk_installation.log
    if [ $? -eq 0 ]; then
      echo "JDK installed successfully."
    else
      echo "JDK installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/jdk_installation.log \n"
  }

  # Kubectl Installation
  {
    printf "\n Installing kubectl..."
    install_kubectl > /tmp/kubectl_installation.log
    if [ $? -eq 0 ]; then
      echo "kubectl installed successfully."
    else
      echo "kubectl installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/kubectl_installation.log \n"
  }

  # Bazel Installation
  {
    printf "\n Installing bazel..."
    install_bazel > /tmp/bazel_installation.log
    if [ $? -eq 0 ]; then
      echo "Bazel installed successfully."
    else
      echo "Bazel installation Failed with a return code $?"
      exit $?
    fi
    printf " Installation logs in /tmp/bazel_installation.log \n"
  }

  # Clone application and Infra code
  {
    printf "\n Cloning repository cross-media-measurement..."
    clone_repo > /tmp/git_clone.log
    if [ $? -eq 0 ]; then
      echo "Cloned application code Successfully"
    else
      echo "git clone failed $?"
      exit $?
    fi
    printf " Clone logs in /tmp/git_clone.log \n\n"
  }

  # Bazel build
  {
    printf " Image creation Triggered in the background /tmp/build_image.log \n"
    bazel_build > /tmp/build_image.log
    printf " Quite Likely completed. \n\n"
  }

  # Bazel Push
  {
    Printf " Consolidating results to Push the Image..."
    bazel_push > /tmp/push_image.log
    printf " Quite Likely push completed. \n"
  }

  printf "\n Initialization Completed successfully \n\n "

} > /tmp/init_script.log